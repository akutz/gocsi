package main

import (
	"context"
	"flag"
	"fmt"
	"html/template"
	"os"

	"github.com/codedellemc/gocsi"
	"github.com/codedellemc/gocsi/csi"
	"google.golang.org/grpc"
)

var nodeCmds = []*cmd{
	&cmd{
		Name:    "nodepublishvolume",
		Aliases: []string{"mnt", "mount"},
		Action:  nodePublishVolume,
		Flags:   flagsNodePublishVolume,
	},
	&cmd{
		Name:    "nodeunpublishvolume",
		Aliases: []string{"umount", "unmount"},
		Action:  nodeUnpublishVolume,
		Flags:   flagsNodeUnpublishVolume,
	},
	&cmd{
		Name:    "getnodeid",
		Aliases: []string{"id", "getn", "nodeid"},
		Action:  getNodeID,
		Flags:   flagsGetNodeID,
	},
	&cmd{
		Name:    "probenode",
		Aliases: []string{"p", "probe"},
		Action:  probeNode,
		Flags:   flagsProbeNode,
	},
	&cmd{
		Name:    "nodegetcapabilities",
		Aliases: []string{"n", "node", "nget"},
		Action:  nodeGetCapabilities,
		Flags:   flagsNodeGetCapabilities,
	},
}

///////////////////////////////////////////////////////////////////////////////
//                            NodePublishVolume                              //
///////////////////////////////////////////////////////////////////////////////
var argsNodePublishVolume struct {
	publishVolumeInfo mapOfStringArg
	targetPath        string
	fsType            string
	mntFlags          stringSliceArg
	readOnly          bool
	mode              int64
	block             bool
}

func flagsNodePublishVolume(
	ctx context.Context, rpc string) *flag.FlagSet {

	fs := flag.NewFlagSet(rpc, flag.ExitOnError)
	flagsGlobal(fs, "", "")

	fs.Var(
		&argsNodePublishVolume.publishVolumeInfo,
		"publishVolumeInfo",
		"The published volume info to use.")

	fs.StringVar(
		&argsNodePublishVolume.targetPath,
		"targetPath",
		"",
		"The path to which the volume will be published.")

	fs.BoolVar(
		&argsNodePublishVolume.block,
		"block",
		false,
		"A flag that marks the volume for raw device access")

	fs.Int64Var(
		&argsNodePublishVolume.mode,
		"mode",
		0,
		"The volume access mode")

	fs.StringVar(
		&argsNodePublishVolume.fsType,
		"t",
		"",
		"The file system type")

	fs.Var(
		&argsNodePublishVolume.mntFlags,
		"o",
		"The mount flags")

	fs.BoolVar(
		&argsNodePublishVolume.readOnly,
		"ro",
		false,
		"A flag indicating whether or not to "+
			"publish the volume in read-only mode.")

	fs.Usage = newVolumeRPCUsage(rpc, fs)

	return fs
}

func nodePublishVolume(
	ctx context.Context,
	fs *flag.FlagSet,
	cc *grpc.ClientConn) error {

	var (
		err    error
		client csi.NodeClient

		handle     *csi.VolumeHandle
		pubVolInfo *csi.PublishVolumeInfo
		mode       csi.VolumeCapability_AccessMode_Mode
		capability *csi.VolumeCapability

		block    = argsNodePublishVolume.block
		fsType   = argsNodePublishVolume.fsType
		mntFlags = argsNodePublishVolume.mntFlags.vals

		targetPath = argsNodePublishVolume.targetPath
		readOnly   = argsNodePublishVolume.readOnly

		version = args.version
	)

	// Parse the VolumeHandle from the remaining CLI args.
	if handle, err = parseVolumeHandle(fs); err != nil {
		return err
	}

	// make sure maxEntries doesn't exceed int32
	if max := argsNodePublishVolume.mode; max > maxInt32 {
		return fmt.Errorf("error: max entries > int32: %v", max)
	}
	mode = csi.VolumeCapability_AccessMode_Mode(argsNodePublishVolume.mode)

	if block {
		capability = gocsi.NewBlockCapability(mode)
	} else {
		capability = gocsi.NewMountCapability(mode, fsType, mntFlags)
	}

	// check for publish volume info
	if v := argsNodePublishVolume.publishVolumeInfo.vals; len(v) > 0 {
		pubVolInfo = &csi.PublishVolumeInfo{Values: v}
	}

	// initialize the csi client
	client = csi.NewNodeClient(cc)

	// execute the rpc
	if err = gocsi.NodePublishVolume(
		ctx, client, version, handle,
		pubVolInfo, targetPath, capability, readOnly); err != nil {
		return err
	}

	fmt.Println(targetPath)

	return nil
}

///////////////////////////////////////////////////////////////////////////////
//                           NodeUnpublishVolume                             //
///////////////////////////////////////////////////////////////////////////////
var argsNodeUnpublishVolume struct {
	targetPath string
}

func flagsNodeUnpublishVolume(
	ctx context.Context, rpc string) *flag.FlagSet {

	fs := flag.NewFlagSet(rpc, flag.ExitOnError)
	flagsGlobal(fs, volHandleFormat, "*csi.VolumeHandle")

	fs.StringVar(
		&argsNodeUnpublishVolume.targetPath,
		"targetPath",
		"",
		"The path to which the volume is published.")

	fs.Usage = newVolumeRPCUsage(rpc, fs)
	return fs
}

func nodeUnpublishVolume(
	ctx context.Context,
	fs *flag.FlagSet,
	cc *grpc.ClientConn) error {

	var (
		err    error
		client csi.NodeClient
		handle *csi.VolumeHandle
		tpl    *template.Template

		targetPath = argsNodeUnpublishVolume.targetPath

		version = args.version
	)

	// Create a template for emitting the output.
	tpl = template.New("template")
	if tpl, err = tpl.Parse(args.format); err != nil {
		return err
	}

	// Parse the VolumeHandle from the remaining CLI args.
	if handle, err = parseVolumeHandle(fs); err != nil {
		return err
	}

	// initialize the csi client
	client = csi.NewNodeClient(cc)

	// execute the rpc
	if err := gocsi.NodeUnpublishVolume(
		ctx, client, version, handle, targetPath); err != nil {
		return err
	}

	// If the RPC is successful then emit the volume handle.
	if err = tpl.Execute(os.Stdout, handle); err != nil {
		return err
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////
//                                GetNodeID                                  //
///////////////////////////////////////////////////////////////////////////////
func flagsGetNodeID(
	ctx context.Context, rpc string) *flag.FlagSet {

	fs := flag.NewFlagSet(rpc, flag.ExitOnError)
	flagsGlobal(fs, mapSzOfSzFormat, "map[string]string")

	fs.Usage = func() {
		fmt.Fprintf(
			os.Stderr,
			"usage: %s %s [ARGS...]\n",
			appName, rpc)
		fs.PrintDefaults()
	}
	return fs
}

func getNodeID(
	ctx context.Context,
	fs *flag.FlagSet,
	cc *grpc.ClientConn) error {

	var (
		err     error
		client  csi.NodeClient
		tpl     *template.Template
		nodeID  *csi.NodeID
		version = args.version
		format  = args.format
	)

	// create a template for emitting the output
	tpl = template.New("template")
	if tpl, err = tpl.Parse(format); err != nil {
		return err
	}

	// initialize the csi client
	client = csi.NewNodeClient(cc)

	// execute the rpc
	if nodeID, err = gocsi.GetNodeID(ctx, client, version); err != nil {
		return err
	}

	// emit the result
	if err = tpl.Execute(
		os.Stdout, nodeID.GetValues()); err != nil {
		return err
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////
//                                ProbeNode                                  //
///////////////////////////////////////////////////////////////////////////////
func flagsProbeNode(
	ctx context.Context, rpc string) *flag.FlagSet {

	fs := flag.NewFlagSet(rpc, flag.ExitOnError)
	flagsGlobal(fs, "", "")

	fs.Usage = func() {
		fmt.Fprintf(
			os.Stderr,
			"usage: %s %s [ARGS...]\n",
			appName, rpc)
		fs.PrintDefaults()
	}
	return fs
}

func probeNode(
	ctx context.Context,
	fs *flag.FlagSet,
	cc *grpc.ClientConn) error {

	// initialize the csi client
	client := csi.NewNodeClient(cc)

	// execute the rpc
	err := gocsi.ProbeNode(ctx, client, args.version)
	if err != nil {
		return err
	}

	fmt.Println("Success")

	return nil
}

///////////////////////////////////////////////////////////////////////////////
//                              NodeGetCapabilities                          //
///////////////////////////////////////////////////////////////////////////////
func flagsNodeGetCapabilities(
	ctx context.Context, rpc string) *flag.FlagSet {

	fs := flag.NewFlagSet(rpc, flag.ExitOnError)
	flagsGlobal(fs, capFormat, "[]*csi.NodeServiceCapability")

	fs.Usage = func() {
		fmt.Fprintf(
			os.Stderr,
			"usage: %s %s [ARGS...]\n",
			appName, rpc)
		fs.PrintDefaults()
	}
	return fs
}

func nodeGetCapabilities(
	ctx context.Context,
	fs *flag.FlagSet,
	cc *grpc.ClientConn) error {

	// initialize the csi client
	client := csi.NewNodeClient(cc)

	// execute the rpc
	caps, err := gocsi.NodeGetCapabilities(ctx, client, args.version)
	if err != nil {
		return err
	}

	// create a template for emitting the output
	tpl := template.New("template")
	if tpl, err = tpl.Parse(args.format); err != nil {
		return err
	}
	// emit the results
	for _, c := range caps {
		if err = tpl.Execute(
			os.Stdout, c); err != nil {
			return err
		}
	}

	return nil
}
