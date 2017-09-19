package gocsi

import (
	"fmt"

	"github.com/codedellemc/gocsi/csi"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type serverRequestValidator struct {
}

// NewServerRequestValidator returns a new UnaryServerInterceptor that
// validates server request data.
func NewServerRequestValidator() grpc.UnaryServerInterceptor {
	return (&serverRequestValidator{}).handle
}

// ServerRequestValidator is a UnaryServerInterceptor that validates
// server request data.
func (s *serverRequestValidator) handle(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {

	fullMethod := info.FullMethod

	switch treq := req.(type) {

	// Controller
	case *csi.CreateVolumeRequest:
		rep, err := s.createVolume(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.DeleteVolumeRequest:
		rep, err := s.deleteVolume(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.ControllerPublishVolumeRequest:
		rep, err := s.controllerPublishVolume(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.ControllerUnpublishVolumeRequest:
		rep, err := s.controllerUnpublishVolume(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.ValidateVolumeCapabilitiesRequest:
		rep, err := s.validateVolumeCapabilities(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.ListVolumesRequest:
		rep, err := s.listVolumes(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.GetCapacityRequest:
		rep, err := s.getCapacity(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.ControllerGetCapabilitiesRequest:
		rep, err := s.controllerGetCapabilities(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}

	// Identity
	case *csi.GetSupportedVersionsRequest:
		rep, err := s.getSupportedVersions(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.GetPluginInfoRequest:
		rep, err := s.getPluginInfo(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}

	// Node
	case *csi.NodePublishVolumeRequest:
		rep, err := s.nodePublishVolume(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.NodeUnpublishVolumeRequest:
		rep, err := s.nodeUnpublishVolume(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.GetNodeIDRequest:
		rep, err := s.getNodeID(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.ProbeNodeRequest:
		rep, err := s.probeNode(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	case *csi.NodeGetCapabilitiesRequest:
		rep, err := s.nodeGetCapabilities(ctx, fullMethod, treq)
		if err != nil {
			return nil, err
		}
		if rep != nil {
			return rep, nil
		}
	}

	return handler(ctx, req)
}

////////////////////////////////////////////////////////////////////////////////
//                      SERVER REQUEST - CONTROLLER                           //
////////////////////////////////////////////////////////////////////////////////

func (s *serverRequestValidator) createVolume(
	ctx context.Context,
	method string,
	req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {

	if req.Name == "" {
		return ErrCreateVolume(
			csi.Error_CreateVolumeError_INVALID_VOLUME_NAME,
			"missing name"), nil
	}

	if len(req.VolumeCapabilities) == 0 {
		return ErrCreateVolume(
			csi.Error_CreateVolumeError_UNKNOWN,
			"missing volume capabilities"), nil
	}

	for i, cap := range req.VolumeCapabilities {
		if cap.AccessMode == nil {
			return ErrCreateVolume(
				csi.Error_CreateVolumeError_UNKNOWN,
				fmt.Sprintf("missing access mode: index %d", i)), nil
		}
		atype := cap.GetAccessType()
		if atype == nil {
			return ErrCreateVolume(
				csi.Error_CreateVolumeError_UNKNOWN,
				fmt.Sprintf("missing access type: index %d", i)), nil
		}
		switch tatype := atype.(type) {
		case *csi.VolumeCapability_Block:
			if tatype.Block == nil {
				return ErrCreateVolume(
					csi.Error_CreateVolumeError_UNKNOWN,
					fmt.Sprintf("missing block type: index %d", i)), nil
			}
		case *csi.VolumeCapability_Mount:
			if tatype.Mount == nil {
				return ErrCreateVolume(
					csi.Error_CreateVolumeError_UNKNOWN,
					fmt.Sprintf("missing mount type: index %d", i)), nil
			}
		default:
			return ErrCreateVolume(
				csi.Error_CreateVolumeError_UNKNOWN,
				fmt.Sprintf(
					"invalid access type: index %d, type=%T",
					i, atype)), nil
		}
	}

	if req.UserCredentials != nil && len(req.UserCredentials.Data) == 0 {
		return ErrCreateVolumeGeneral(
			csi.Error_GeneralError_MISSING_REQUIRED_FIELD,
			"empty credentials package specified"), nil
	}

	return nil, nil
}

func (s *serverRequestValidator) deleteVolume(
	ctx context.Context,
	method string,
	req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {

	if req.VolumeHandle == nil {
		return ErrDeleteVolume(
			csi.Error_DeleteVolumeError_INVALID_VOLUME_HANDLE,
			ErrNilVolumeHandle.Error()), nil
	}

	if req.VolumeHandle.Id == "" {
		return ErrDeleteVolume(
			csi.Error_DeleteVolumeError_INVALID_VOLUME_HANDLE,
			ErrEmptyVolumeID.Error()), nil
	}

	if req.VolumeHandle.Metadata != nil && len(req.VolumeHandle.Metadata) == 0 {
		return ErrDeleteVolume(
			csi.Error_DeleteVolumeError_INVALID_VOLUME_HANDLE,
			ErrNonNilEmptyMetadata.Error()), nil
	}

	if req.UserCredentials != nil && len(req.UserCredentials.Data) == 0 {
		return ErrDeleteVolumeGeneral(
			csi.Error_GeneralError_MISSING_REQUIRED_FIELD,
			"empty credentials package specified"), nil
	}

	return nil, nil
}

func (s *serverRequestValidator) controllerPublishVolume(
	ctx context.Context,
	method string,
	req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {

	if req.VolumeHandle == nil {
		return ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrNilVolumeHandle.Error()), nil
	}

	if req.VolumeHandle.Id == "" {
		return ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrEmptyVolumeID.Error()), nil
	}

	if req.VolumeHandle.Metadata != nil && len(req.VolumeHandle.Metadata) == 0 {
		return ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrNonNilEmptyMetadata.Error()), nil
	}

	if req.VolumeCapability == nil {
		return ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_UNKNOWN,
			"missing volume capability"), nil
	}

	if req.VolumeCapability.AccessMode == nil {
		return ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_UNKNOWN,
			"missing access mode"), nil
	}
	atype := req.VolumeCapability.GetAccessType()
	if atype == nil {
		return ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_UNKNOWN,
			"missing access type"), nil
	}
	switch tatype := atype.(type) {
	case *csi.VolumeCapability_Block:
		if tatype.Block == nil {
			return ErrControllerPublishVolume(
				csi.Error_ControllerPublishVolumeError_UNKNOWN,
				"missing block type"), nil
		}
	case *csi.VolumeCapability_Mount:
		if tatype.Mount == nil {
			return ErrControllerPublishVolume(
				csi.Error_ControllerPublishVolumeError_UNKNOWN,
				"missing mount type"), nil
		}
	default:
		return ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_UNKNOWN,
			fmt.Sprintf("invalid access type: %T", atype)), nil
	}

	if req.UserCredentials != nil && len(req.UserCredentials.Data) == 0 {
		return ErrControllerPublishVolumeGeneral(
			csi.Error_GeneralError_MISSING_REQUIRED_FIELD,
			"empty credentials package specified"), nil
	}

	if req.NodeId != nil && len(req.NodeId.Values) == 0 {
		return ErrControllerPublishVolume(
			csi.Error_ControllerPublishVolumeError_INVALID_NODE_ID, ""), nil
	}

	return nil, nil
}

func (s *serverRequestValidator) controllerUnpublishVolume(
	ctx context.Context,
	method string,
	req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {

	if req.VolumeHandle == nil {
		return ErrControllerUnpublishVolume(
			csi.Error_ControllerUnpublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrNilVolumeHandle.Error()), nil
	}

	if req.VolumeHandle.Id == "" {
		return ErrControllerUnpublishVolume(
			csi.Error_ControllerUnpublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrEmptyVolumeID.Error()), nil
	}

	if req.VolumeHandle.Metadata != nil && len(req.VolumeHandle.Metadata) == 0 {
		return ErrControllerUnpublishVolume(
			csi.Error_ControllerUnpublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrNonNilEmptyMetadata.Error()), nil
	}

	if req.UserCredentials != nil && len(req.UserCredentials.Data) == 0 {
		return ErrControllerUnpublishVolumeGeneral(
			csi.Error_GeneralError_MISSING_REQUIRED_FIELD,
			"empty credentials package specified"), nil
	}

	if req.NodeId != nil && len(req.NodeId.Values) == 0 {
		return ErrControllerUnpublishVolume(
			csi.Error_ControllerUnpublishVolumeError_INVALID_NODE_ID, ""), nil
	}

	return nil, nil
}

func (s *serverRequestValidator) validateVolumeCapabilities(
	ctx context.Context,
	method string,
	req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	if req.VolumeInfo == nil {
		return ErrValidateVolumeCapabilities(
			csi.Error_ValidateVolumeCapabilitiesError_INVALID_VOLUME_INFO,
			ErrNilVolumeInfo.Error()), nil
	}

	if req.VolumeInfo.Handle == nil {
		return ErrValidateVolumeCapabilities(
			csi.Error_ValidateVolumeCapabilitiesError_INVALID_VOLUME_INFO,
			ErrNilVolumeHandle.Error()), nil
	}

	if req.VolumeInfo.Handle.Id == "" {
		return ErrValidateVolumeCapabilities(
			csi.Error_ValidateVolumeCapabilitiesError_INVALID_VOLUME_INFO,
			ErrEmptyVolumeID.Error()), nil
	}

	if req.VolumeInfo.Handle.Metadata != nil &&
		len(req.VolumeInfo.Handle.Metadata) == 0 {
		return ErrValidateVolumeCapabilities(
			csi.Error_ValidateVolumeCapabilitiesError_INVALID_VOLUME_INFO,
			ErrNonNilEmptyMetadata.Error()), nil
	}

	if len(req.VolumeCapabilities) == 0 {
		return ErrValidateVolumeCapabilities(
			csi.Error_ValidateVolumeCapabilitiesError_UNKNOWN,
			"missing volume capabilities"), nil
	}

	for i, cap := range req.VolumeCapabilities {
		if cap.AccessMode == nil {
			return ErrValidateVolumeCapabilities(
				csi.Error_ValidateVolumeCapabilitiesError_UNKNOWN,
				fmt.Sprintf("missing access mode: index %d", i)), nil
		}
		atype := cap.GetAccessType()
		if atype == nil {
			return ErrValidateVolumeCapabilities(
				csi.Error_ValidateVolumeCapabilitiesError_UNKNOWN,
				fmt.Sprintf("missing access type: index %d", i)), nil
		}
		switch tatype := atype.(type) {
		case *csi.VolumeCapability_Block:
			if tatype.Block == nil {
				return ErrValidateVolumeCapabilities(
					csi.Error_ValidateVolumeCapabilitiesError_UNKNOWN,
					fmt.Sprintf("missing block type: index %d", i)), nil
			}
		case *csi.VolumeCapability_Mount:
			if tatype.Mount == nil {
				return ErrValidateVolumeCapabilities(
					csi.Error_ValidateVolumeCapabilitiesError_UNKNOWN,
					fmt.Sprintf("missing mount type: index %d", i)), nil
			}
		default:
			return ErrValidateVolumeCapabilities(
				csi.Error_ValidateVolumeCapabilitiesError_UNKNOWN,
				fmt.Sprintf(
					"invalid access type: index %d, type=%T",
					i, atype)), nil
		}
	}

	return nil, nil
}

func (s *serverRequestValidator) listVolumes(
	ctx context.Context,
	method string,
	req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {

	return nil, nil
}

func (s *serverRequestValidator) getCapacity(
	ctx context.Context,
	method string,
	req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {

	if len(req.VolumeCapabilities) == 0 {
		return nil, nil
	}

	for i, cap := range req.VolumeCapabilities {
		if cap.AccessMode == nil {
			return ErrGetCapacity(
				csi.Error_GeneralError_UNDEFINED,
				fmt.Sprintf("missing access mode: index %d", i)), nil
		}
		atype := cap.GetAccessType()
		if atype == nil {
			return ErrGetCapacity(
				csi.Error_GeneralError_UNDEFINED,
				fmt.Sprintf("missing access type: index %d", i)), nil
		}
		switch tatype := atype.(type) {
		case *csi.VolumeCapability_Block:
			if tatype.Block == nil {
				return ErrGetCapacity(
					csi.Error_GeneralError_UNDEFINED,
					fmt.Sprintf("missing block type: index %d", i)), nil
			}
		case *csi.VolumeCapability_Mount:
			if tatype.Mount == nil {
				return ErrGetCapacity(
					csi.Error_GeneralError_UNDEFINED,
					fmt.Sprintf("missing mount type: index %d", i)), nil
			}
		default:
			return ErrGetCapacity(
				csi.Error_GeneralError_UNDEFINED,
				fmt.Sprintf(
					"invalid access type: index %d, type=%T",
					i, atype)), nil
		}
	}

	return nil, nil
}

func (s *serverRequestValidator) controllerGetCapabilities(
	ctx context.Context,
	method string,
	req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {

	return nil, nil
}

////////////////////////////////////////////////////////////////////////////////
//                        SERVER REQUEST - IDENTITY                           //
////////////////////////////////////////////////////////////////////////////////

func (s *serverRequestValidator) getSupportedVersions(
	ctx context.Context,
	method string,
	req *csi.GetSupportedVersionsRequest) (
	*csi.GetSupportedVersionsResponse, error) {

	return nil, nil
}

func (s *serverRequestValidator) getPluginInfo(
	ctx context.Context,
	method string,
	req *csi.GetPluginInfoRequest) (
	*csi.GetPluginInfoResponse, error) {

	return nil, nil
}

////////////////////////////////////////////////////////////////////////////////
//                         SERVER REQUEST - NODE                              //
////////////////////////////////////////////////////////////////////////////////

func (s *serverRequestValidator) nodePublishVolume(
	ctx context.Context,
	method string,
	req *csi.NodePublishVolumeRequest) (
	*csi.NodePublishVolumeResponse, error) {

	if req.VolumeHandle == nil {
		return ErrNodePublishVolume(
			csi.Error_NodePublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrNilVolumeHandle.Error()), nil
	}

	if req.VolumeHandle.Id == "" {
		return ErrNodePublishVolume(
			csi.Error_NodePublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrEmptyVolumeID.Error()), nil
	}

	if req.VolumeHandle.Metadata != nil && len(req.VolumeHandle.Metadata) == 0 {
		return ErrNodePublishVolume(
			csi.Error_NodePublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrNonNilEmptyMetadata.Error()), nil
	}

	if req.VolumeCapability == nil {
		return ErrNodePublishVolume(
			csi.Error_NodePublishVolumeError_UNKNOWN,
			"missing volume capability"), nil
	}

	if req.VolumeCapability.AccessMode == nil {
		return ErrNodePublishVolume(
			csi.Error_NodePublishVolumeError_UNKNOWN,
			"missing access mode"), nil
	}
	atype := req.VolumeCapability.GetAccessType()
	if atype == nil {
		return ErrNodePublishVolume(
			csi.Error_NodePublishVolumeError_UNKNOWN,
			"missing access type"), nil
	}
	switch tatype := atype.(type) {
	case *csi.VolumeCapability_Block:
		if tatype.Block == nil {
			return ErrNodePublishVolume(
				csi.Error_NodePublishVolumeError_UNKNOWN,
				"missing block type"), nil
		}
	case *csi.VolumeCapability_Mount:
		if tatype.Mount == nil {
			return ErrNodePublishVolume(
				csi.Error_NodePublishVolumeError_UNKNOWN,
				"missing mount type"), nil
		}
	default:
		return ErrNodePublishVolume(
			csi.Error_NodePublishVolumeError_UNKNOWN,
			fmt.Sprintf("invalid access type: %T", atype)), nil
	}

	if req.TargetPath == "" {
		return ErrNodePublishVolume(
			csi.Error_NodePublishVolumeError_UNKNOWN,
			"missing target path"), nil
	}

	if req.UserCredentials != nil && len(req.UserCredentials.Data) == 0 {
		return ErrNodePublishVolumeGeneral(
			csi.Error_GeneralError_MISSING_REQUIRED_FIELD,
			"empty credentials package specified"), nil
	}

	return nil, nil
}

func (s *serverRequestValidator) nodeUnpublishVolume(
	ctx context.Context,
	method string,
	req *csi.NodeUnpublishVolumeRequest) (
	*csi.NodeUnpublishVolumeResponse, error) {

	if req.VolumeHandle == nil {
		return ErrNodeUnpublishVolume(
			csi.Error_NodeUnpublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrNilVolumeHandle.Error()), nil
	}

	if req.VolumeHandle.Id == "" {
		return ErrNodeUnpublishVolume(
			csi.Error_NodeUnpublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrEmptyVolumeID.Error()), nil
	}

	if req.VolumeHandle.Metadata != nil && len(req.VolumeHandle.Metadata) == 0 {
		return ErrNodeUnpublishVolume(
			csi.Error_NodeUnpublishVolumeError_INVALID_VOLUME_HANDLE,
			ErrNonNilEmptyMetadata.Error()), nil
	}

	if req.TargetPath == "" {
		return ErrNodeUnpublishVolume(
			csi.Error_NodeUnpublishVolumeError_UNKNOWN,
			"missing target path"), nil
	}

	if req.UserCredentials != nil && len(req.UserCredentials.Data) == 0 {
		return ErrNodeUnpublishVolumeGeneral(
			csi.Error_GeneralError_MISSING_REQUIRED_FIELD,
			"empty credentials package specified"), nil
	}

	return nil, nil
}

func (s *serverRequestValidator) getNodeID(
	ctx context.Context,
	method string,
	req *csi.GetNodeIDRequest) (
	*csi.GetNodeIDResponse, error) {

	return nil, nil
}

func (s *serverRequestValidator) probeNode(
	ctx context.Context,
	method string,
	req *csi.ProbeNodeRequest) (
	*csi.ProbeNodeResponse, error) {

	return nil, nil
}

func (s *serverRequestValidator) nodeGetCapabilities(
	ctx context.Context,
	method string,
	req *csi.NodeGetCapabilitiesRequest) (
	*csi.NodeGetCapabilitiesResponse, error) {

	return nil, nil
}
