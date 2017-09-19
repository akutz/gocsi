package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/codedellemc/gocsi"
	"github.com/codedellemc/gocsi/csi"
)

// Define some loggers.
var (
	louti = log.New(os.Stdout, "INFO  ", log.LstdFlags|log.Lshortfile)
	lerri = log.New(os.Stderr, "INFO  ", log.LstdFlags|log.Lshortfile)
	lerre = log.New(os.Stderr, "ERROR ", log.LstdFlags|log.Lshortfile)
)

////////////////////////////////////////////////////////////////////////////////
//                                 CLI                                        //
////////////////////////////////////////////////////////////////////////////////

func main() {
	l, err := gocsi.GetCSIEndpointListener()
	if err != nil {
		lerre.Fatalf("failed to listen: %v\n", err)
	}

	// Define a lambda that can be used in the exit handler
	// to remove a potential UNIX sock file.
	rmSockFile := func() {
		if l == nil || l.Addr() == nil {
			return
		}
		if l.Addr().Network() == "unix" {
			sockFile := l.Addr().String()
			os.RemoveAll(sockFile)
			lerri.Printf("removed sock file: %s\n", sockFile)
		}
	}

	ctx := context.Background()

	s := &sp{name: "mock"}

	trapSignals(func() {
		s.GracefulStop(ctx)
		rmSockFile()
		lerri.Println("server stopped gracefully")
	}, func() {
		s.Stop(ctx)
		rmSockFile()
		lerri.Println("server aborted")
	})

	if err := s.Serve(ctx, l); err != nil {
		lerre.Printf("grpc failed: %v\n", err)
	}
}

func trapSignals(onExit, onAbort func()) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc)
	go func() {
		for s := range sigc {
			ok, graceful := isExitSignal(s)
			if !ok {
				continue
			}
			if !graceful {
				lerri.Printf("received signal: %v: aborting\n", s)
				if onAbort != nil {
					onAbort()
				}
				os.Exit(1)
			}
			lerri.Printf("received signal: %v: shutting down\n", s)
			if onExit != nil {
				onExit()
			}
			os.Exit(0)
		}
	}()
}

// isExitSignal returns a flag indicating whether a signal is SIGKILL, SIGHUP,
// SIGINT, SIGTERM, or SIGQUIT. The second return value is whether it is a
// graceful exit. This flag is true for SIGTERM, SIGHUP, SIGINT, and SIGQUIT.
func isExitSignal(s os.Signal) (bool, bool) {
	switch s {
	case syscall.SIGKILL:
		return true, false
	case syscall.SIGTERM,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGQUIT:
		return true, true
	default:
		return false, false
	}
}

var (
	errServerStarted = errors.New("gocsi: the server has been started")
	errServerStopped = errors.New("gocsi: the server has been stopped")
)

type sp struct {
	sync.Mutex
	name   string
	server *grpc.Server
	closed bool
}

func newGrpcServer() *grpc.Server {
	lout := newLogger(louti.Printf)
	lerr := newLogger(lerre.Printf)
	return grpc.NewServer(grpc.UnaryInterceptor(gocsi.ChainUnaryServer(
		gocsi.ServerRequestIDInjector,
		gocsi.NewServerRequestLogger(lout, lerr),
		gocsi.NewServerResponseLogger(lout, lerr),
		gocsi.NewServerRequestVersionValidator(supportedVersions),
		gocsi.NewServerRequestValidator())))
}

// ServiceProvider.Serve
func (s *sp) Serve(ctx context.Context, li net.Listener) error {
	if err := func() error {
		s.Lock()
		defer s.Unlock()
		if s.closed {
			return errServerStopped
		}
		if s.server != nil {
			return errServerStarted
		}
		louti.Printf("%s.Serve: %s\n", s.name, li.Addr())
		s.server = newGrpcServer()
		return nil
	}(); err != nil {
		return errServerStarted
	}

	csi.RegisterControllerServer(s.server, s)
	csi.RegisterIdentityServer(s.server, s)
	csi.RegisterNodeServer(s.server, s)

	// start the grpc server
	if err := s.server.Serve(li); err != grpc.ErrServerStopped {
		return err
	}
	return errServerStopped
}

//  ServiceProvider.Stop
func (s *sp) Stop(ctx context.Context) {
	louti.Printf("%s.Stop\n", s.name)
	s.Lock()
	defer s.Unlock()

	if s.closed || s.server == nil {
		return
	}
	s.server.Stop()
	s.server = nil
	s.closed = true
}

//  ServiceProvider.GracefulStop
func (s *sp) GracefulStop(ctx context.Context) {
	louti.Printf("%s.GracefulStop\n", s.name)
	s.Lock()
	defer s.Unlock()

	if s.closed || s.server == nil {
		return
	}
	s.server.GracefulStop()
	s.server = nil
	s.closed = true
}

////////////////////////////////////////////////////////////////////////////////
//                            Controller Service                              //
////////////////////////////////////////////////////////////////////////////////

func (s *sp) CreateVolume(
	ctx context.Context,
	req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {

	// New volumes have a default capacity of 100GiB
	capacity := gib100

	// If the request specifies a capacity then use that.
	if cr := req.CapacityRange; cr != nil {
		if rb := cr.RequiredBytes; rb > 0 {
			capacity = rb
		}
		if lb := cr.LimitBytes; lb > 0 {
			capacity = lb
		}
	}

	// Create the volume.
	v := newVolume(req.Name, capacity)

	// Append the new volume to the global list of volumes.
	func() {
		volsRWL.Lock()
		defer volsRWL.Unlock()
		vols = append(vols, v)
	}()

	return &csi.CreateVolumeResponse{
		Reply: &csi.CreateVolumeResponse_Result_{
			Result: &csi.CreateVolumeResponse_Result{
				VolumeInfo: v,
			},
		},
	}, nil
}

func (s *sp) DeleteVolume(
	ctx context.Context,
	req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {

	// Find the volume's index in the global volume list.
	x, _ := findVol(req.VolumeHandle)

	// This delete logic won't preserve order,
	// but it will prevent any potential mem
	// leaks due to orphaned references
	func() {
		volsRWL.Lock()
		defer volsRWL.Unlock()
		vols[x] = vols[len(vols)-1]
		vols[len(vols)-1] = nil
		vols = vols[:len(vols)-1]
	}()

	return &csi.DeleteVolumeResponse{
		Reply: &csi.DeleteVolumeResponse_Result_{
			Result: &csi.DeleteVolumeResponse_Result{},
		},
	}, nil
}

func (s *sp) ControllerPublishVolume(
	ctx context.Context,
	req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {

	if req.NodeId == nil {
		return gocsi.ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_INVALID_NODE_ID,
			"nil node id"), nil
	}

	nodeID, ok := req.NodeId.Values["id"]
	if !ok {
		return gocsi.ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_INVALID_NODE_ID,
			"missing node id"), nil
	}

	// The key used with the volume's metadata to see if the volume
	// is attached to a given node id
	attk := fmt.Sprintf("devpath.%s", nodeID)

	// Get the volume from the global list.
	_, v := findVol(req.VolumeHandle)

	// a "new" device path
	var devpath string

	// Check to see if the volume is attached to this nods. if it
	// is then return the existing dev path
	if p, ok := v.Handle.Metadata[attk]; ok {
		devpath = p
	} else {
		// Attach the volume
		devpath = fmt.Sprintf("%d", time.Now().UTC().Unix())
		v.Handle.Metadata[attk] = devpath
	}

	resp := &csi.ControllerPublishVolumeResponse{
		Reply: &csi.ControllerPublishVolumeResponse_Result_{
			Result: &csi.ControllerPublishVolumeResponse_Result{
				PublishVolumeInfo: &csi.PublishVolumeInfo{
					Values: map[string]string{
						"devpath": devpath,
					},
				},
			},
		},
	}

	return resp, nil
}

func (s *sp) ControllerUnpublishVolume(
	ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {

	if req.NodeId == nil {
		return gocsi.ErrControllerUnpublishVolume(
			csi.Error_ControllerUnpublishVolumeError_INVALID_NODE_ID,
			"nil node id"), nil
	}

	nodeID, ok := req.NodeId.Values["id"]
	if !ok {
		return gocsi.ErrControllerUnpublishVolume(
			csi.Error_ControllerUnpublishVolumeError_INVALID_NODE_ID,
			"missing node id"), nil
	}

	// The key used with the volume's metadata to see if the volume
	// is attached to a given node id
	attk := fmt.Sprintf("devpath.%s", nodeID)

	// Get the volume from the global list.
	_, v := findVol(req.VolumeHandle)

	// Check to see if the volume is attached to thi node
	if _, ok := v.Handle.Metadata[attk]; !ok {
		return gocsi.ErrControllerUnpublishVolume(
			csi.Error_ControllerUnpublishVolumeError_VOLUME_NOT_ATTACHED_TO_SPECIFIED_NODE,
			"not attached"), nil
	}

	// Zero out the device path for this node
	delete(v.Handle.Metadata, attk)

	return &csi.ControllerUnpublishVolumeResponse{
		Reply: &csi.ControllerUnpublishVolumeResponse_Result_{
			Result: &csi.ControllerUnpublishVolumeResponse_Result{},
		},
	}, nil
}

func (s *sp) ValidateVolumeCapabilities(
	ctx context.Context,
	req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	return &csi.ValidateVolumeCapabilitiesResponse{
		Reply: &csi.ValidateVolumeCapabilitiesResponse_Result_{
			Result: &csi.ValidateVolumeCapabilitiesResponse_Result{
				Supported: true,
				Message:   "Yip yip yip yip!",
			},
		},
	}, nil
}

func (s *sp) ListVolumes(
	ctx context.Context,
	req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {

	var (
		ulenVols      = uint32(len(vols))
		maxEntries    = uint32(req.GetMaxEntries())
		startingToken uint32
	)

	if v := req.StartingToken; v != "" {
		i, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return gocsi.ErrListVolumes(0, fmt.Sprintf(
				"startingToken=%d !< uint32=%d",
				startingToken, math.MaxUint32)), nil
		}
		startingToken = uint32(i)
	}

	if startingToken > ulenVols {
		return gocsi.ErrListVolumes(0, fmt.Sprintf(
			"startingToken=%d > len(vols)=%d",
			startingToken, ulenVols)), nil
	}

	entries := []*csi.ListVolumesResponse_Result_Entry{}
	lena := uint32(0)
	for x := startingToken; x < ulenVols; x++ {
		if maxEntries > 0 && lena >= maxEntries {
			break
		}
		v := vols[x]
		entries = append(entries,
			&csi.ListVolumesResponse_Result_Entry{VolumeInfo: v})
		lena++
	}

	var nextToken string
	if (startingToken + lena) < ulenVols {
		nextToken = fmt.Sprintf("%d", startingToken+lena)
	}

	return &csi.ListVolumesResponse{
		Reply: &csi.ListVolumesResponse_Result_{
			Result: &csi.ListVolumesResponse_Result{
				Entries:   entries,
				NextToken: nextToken,
			},
		},
	}, nil
}

func (s *sp) GetCapacity(
	ctx context.Context,
	req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {

	return &csi.GetCapacityResponse{
		Reply: &csi.GetCapacityResponse_Result_{
			Result: &csi.GetCapacityResponse_Result{
				AvailableCapacity: tib100,
			},
		},
	}, nil
}

func (s *sp) ControllerGetCapabilities(
	ctx context.Context,
	req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {

	return &csi.ControllerGetCapabilitiesResponse{
		Reply: &csi.ControllerGetCapabilitiesResponse_Result_{
			Result: &csi.ControllerGetCapabilitiesResponse_Result{
				Capabilities: []*csi.ControllerServiceCapability{
					&csi.ControllerServiceCapability{
						Type: &csi.ControllerServiceCapability_Rpc{
							Rpc: &csi.ControllerServiceCapability_RPC{
								// CREATE_DELETE_VOLUME
								Type: 1,
							},
						},
					},
					&csi.ControllerServiceCapability{
						Type: &csi.ControllerServiceCapability_Rpc{
							Rpc: &csi.ControllerServiceCapability_RPC{
								// PUBLISH_UNPUBLISH_VOLUME
								Type: 2,
							},
						},
					},
					&csi.ControllerServiceCapability{
						Type: &csi.ControllerServiceCapability_Rpc{
							Rpc: &csi.ControllerServiceCapability_RPC{
								// LIST_VOLUMES
								Type: 3,
							},
						},
					},
					&csi.ControllerServiceCapability{
						Type: &csi.ControllerServiceCapability_Rpc{
							Rpc: &csi.ControllerServiceCapability_RPC{
								// GET_CAPACITY
								Type: 4,
							},
						},
					},
				},
			},
		},
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
//                             Identity Service                               //
////////////////////////////////////////////////////////////////////////////////

var supportedVersions = []*csi.Version{
	&csi.Version{
		Major: 0,
		Minor: 1,
		Patch: 0,
	},
	&csi.Version{
		Major: 0,
		Minor: 2,
		Patch: 0,
	},
	&csi.Version{
		Major: 1,
		Minor: 0,
		Patch: 0,
	},
	&csi.Version{
		Major: 1,
		Minor: 1,
		Patch: 0,
	},
}

func (s *sp) GetSupportedVersions(
	ctx context.Context,
	req *csi.GetSupportedVersionsRequest) (
	*csi.GetSupportedVersionsResponse, error) {

	return &csi.GetSupportedVersionsResponse{
		Reply: &csi.GetSupportedVersionsResponse_Result_{
			Result: &csi.GetSupportedVersionsResponse_Result{
				SupportedVersions: supportedVersions,
			},
		},
	}, nil
}

func (s *sp) GetPluginInfo(
	ctx context.Context,
	req *csi.GetPluginInfoRequest) (
	*csi.GetPluginInfoResponse, error) {

	return &csi.GetPluginInfoResponse{
		Reply: &csi.GetPluginInfoResponse_Result_{
			Result: &csi.GetPluginInfoResponse_Result{
				Name:          s.name,
				VendorVersion: gocsi.SprintfVersion(req.Version),
				Manifest:      nil,
			},
		},
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
//                                Node Service                                //
////////////////////////////////////////////////////////////////////////////////

func (s *sp) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) (
	*csi.NodePublishVolumeResponse, error) {

	_, v := findVol(req.VolumeHandle)

	// record the mount path
	v.Handle.Metadata[nodeMntpath] = req.TargetPath

	return &csi.NodePublishVolumeResponse{
		Reply: &csi.NodePublishVolumeResponse_Result_{
			Result: &csi.NodePublishVolumeResponse_Result{},
		},
	}, nil
}

func (s *sp) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) (
	*csi.NodeUnpublishVolumeResponse, error) {

	_, v := findVol(req.VolumeHandle)

	// zero out the mount path for this node
	delete(v.Handle.Metadata, nodeMntpath)

	return &csi.NodeUnpublishVolumeResponse{
		Reply: &csi.NodeUnpublishVolumeResponse_Result_{
			Result: &csi.NodeUnpublishVolumeResponse_Result{},
		},
	}, nil
}

func (s *sp) GetNodeID(
	ctx context.Context,
	req *csi.GetNodeIDRequest) (
	*csi.GetNodeIDResponse, error) {

	return &csi.GetNodeIDResponse{
		Reply: &csi.GetNodeIDResponse_Result_{
			Result: &csi.GetNodeIDResponse_Result{
				NodeId: nodeID,
			},
		},
	}, nil
}

func (s *sp) ProbeNode(
	ctx context.Context,
	req *csi.ProbeNodeRequest) (
	*csi.ProbeNodeResponse, error) {

	return &csi.ProbeNodeResponse{
		Reply: &csi.ProbeNodeResponse_Result_{
			Result: &csi.ProbeNodeResponse_Result{},
		},
	}, nil
}

func (s *sp) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest) (
	*csi.NodeGetCapabilitiesResponse, error) {

	return &csi.NodeGetCapabilitiesResponse{
		Reply: &csi.NodeGetCapabilitiesResponse_Result_{
			Result: &csi.NodeGetCapabilitiesResponse_Result{
				Capabilities: []*csi.NodeServiceCapability{
					&csi.NodeServiceCapability{
						Type: &csi.NodeServiceCapability_Rpc{
							Rpc: &csi.NodeServiceCapability_RPC{
								Type: csi.NodeServiceCapability_RPC_UNKNOWN,
							},
						},
					},
				},
			},
		},
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
//                                  Utils                                     //
////////////////////////////////////////////////////////////////////////////////

const (
	kib    uint64 = 1024
	mib    uint64 = kib * 1024
	gib    uint64 = mib * 1024
	gib100 uint64 = gib * 100
	tib    uint64 = gib * 1024
	tib100 uint64 = tib * 100

	nodeIDID    = "mock"
	nodeMntpath = nodeIDID + ".mntpath"
	nodeDevpath = nodeIDID + ".devpath"
)

var (
	nextVolID uint64

	vols = []*csi.VolumeInfo{
		newVolume("Mock Volume 1", gib100),
		newVolume("Mock Volume 2", gib100),
		newVolume("Mock Volume 3", gib100),
	}

	volsRWL sync.RWMutex

	nodeID = &csi.NodeID{
		Values: map[string]string{
			"id": nodeIDID,
		},
	}

	version = &csi.Version{Major: 0, Minor: 1, Patch: 0}
)

func newVolume(name string, capcity uint64) *csi.VolumeInfo {
	return &csi.VolumeInfo{
		Handle: &csi.VolumeHandle{
			Id: fmt.Sprintf("%d", atomic.AddUint64(&nextVolID, 1)),
			Metadata: map[string]string{
				"name": name,
			},
		},
		CapacityBytes: capcity,
	}
}

func findVol(h *csi.VolumeHandle) (int, *csi.VolumeInfo) {
	volsRWL.RLock()
	defer volsRWL.RUnlock()

	if h == nil {
		return -1, nil
	}
	if h.Id != "" {
		return findVolByID(h.Id)
	}
	if len(h.Metadata) == 0 {
		return -1, nil
	}
	if n, ok := h.Metadata["name"]; ok {
		return findVolByName(n)
	}
	return -1, nil
}

func findVolByID(id string) (int, *csi.VolumeInfo) {
	return findVolByField("id", id)
}

func findVolByName(name string) (int, *csi.VolumeInfo) {
	return findVolByField("name", name)
}

func findVolByField(field, val string) (int, *csi.VolumeInfo) {
	for x, v := range vols {
		h := v.Handle
		if h == nil {
			continue
		}
		switch field {
		case "id":
			if strings.EqualFold(val, h.Id) {
				return x, v
			}
		case "name":
			n, ok := h.Metadata["name"]
			if ok && strings.EqualFold(val, n) {
				return x, v
			}
		}
	}
	return -1, nil
}

type logger struct {
	f func(msg string, args ...interface{})
	w io.Writer
}

func newLogger(f func(msg string, args ...interface{})) *logger {
	l := &logger{f: f}
	r, w := io.Pipe()
	l.w = w
	go func() {
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			f(scan.Text())
		}
	}()
	return l
}

func (l *logger) Write(data []byte) (int, error) {
	return l.w.Write(data)
}
