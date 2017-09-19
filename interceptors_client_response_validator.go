package gocsi

import (
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/codedellemc/gocsi/csi"
)

type hasGetError interface {
	GetError() *csi.Error
}

type clientResponseValidator struct{}

// NewClientResponseValidator returns a new UnaryClientInterceptor that
// validates server response data.
func NewClientResponseValidator() grpc.UnaryClientInterceptor {
	return (&clientResponseValidator{}).handle
}

func (s *clientResponseValidator) handle(
	ctx context.Context,
	method string,
	req, rep interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption) error {

	// Invoke the call and validate the reply.
	if err := invoker(ctx, method, req, rep, cc, opts...); err != nil {
		return &Error{
			FullMethod: method,
			InnerError: err,
		}
	}

	// Do not validate the reply if it has an error.
	if trep, ok := rep.(hasGetError); ok && trep.GetError() != nil {
		return nil
	}

	switch trep := rep.(type) {

	// Controller
	case *csi.CreateVolumeResponse:
		if err := s.createVolume(ctx, method, trep); err != nil {
			return err
		}
	case *csi.DeleteVolumeResponse:
		if err := s.deleteVolume(ctx, method, trep); err != nil {
			return err
		}
	case *csi.ControllerPublishVolumeResponse:
		if err := s.controllerPublishVolume(ctx, method, trep); err != nil {
			return err
		}
	case *csi.ControllerUnpublishVolumeResponse:
		if err := s.controllerUnpublishVolume(ctx, method, trep); err != nil {
			return err
		}
	case *csi.ValidateVolumeCapabilitiesResponse:
		if err := s.validateVolumeCapabilities(ctx, method, trep); err != nil {
			return err
		}
	case *csi.ListVolumesResponse:
		if err := s.listVolumes(ctx, method, trep); err != nil {
			return err
		}
	case *csi.GetCapacityResponse:
		if err := s.getCapacity(ctx, method, trep); err != nil {
			return err
		}
	case *csi.ControllerGetCapabilitiesResponse:
		if err := s.controllerGetCapabilities(ctx, method, trep); err != nil {
			return err
		}

	// Identity
	case *csi.GetSupportedVersionsResponse:
		if err := s.getSupportedVersions(ctx, method, trep); err != nil {
			return err
		}
	case *csi.GetPluginInfoResponse:
		if err := s.getPluginInfo(ctx, method, trep); err != nil {
			return err
		}

	// Node
	case *csi.NodePublishVolumeResponse:
		if err := s.nodePublishVolume(
			ctx, method, trep); err != nil {
			return err
		}
	case *csi.NodeUnpublishVolumeResponse:
		if err := s.nodeUnpublishVolume(ctx, method, trep); err != nil {
			return err
		}
	case *csi.GetNodeIDResponse:
		if err := s.getNodeID(ctx, method, trep); err != nil {
			return err
		}
	case *csi.ProbeNodeResponse:
		if err := s.probeNode(ctx, method, trep); err != nil {
			return err
		}
	case *csi.NodeGetCapabilitiesResponse:
		if err := s.nodeGetCapabilities(ctx, method, trep); err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
//                     CLIENT RESPONSE - CONTROLLER                           //
////////////////////////////////////////////////////////////////////////////////

func (s *clientResponseValidator) createVolume(
	ctx context.Context,
	method string,
	rep *csi.CreateVolumeResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	volInfo := rep.GetResult().VolumeInfo
	if volInfo == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilVolumeInfo,
		}
	}

	handle := volInfo.Handle
	if handle == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilVolumeHandle,
		}
	}

	if handle.Id == "" {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrEmptyVolumeID,
		}
	}

	if handle.Metadata != nil && len(handle.Metadata) == 0 {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNonNilEmptyMetadata,
		}
	}

	return nil
}

func (s *clientResponseValidator) deleteVolume(
	ctx context.Context,
	method string,
	rep *csi.DeleteVolumeResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

func (s *clientResponseValidator) controllerPublishVolume(
	ctx context.Context,
	method string,
	rep *csi.ControllerPublishVolumeResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	if rep.GetResult().PublishVolumeInfo == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilPublishVolumeInfo,
		}
	}

	if len(rep.GetResult().PublishVolumeInfo.Values) == 0 {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrEmptyPublishVolumeInfo,
		}
	}

	return nil
}

func (s *clientResponseValidator) controllerUnpublishVolume(
	ctx context.Context,
	method string,
	rep *csi.ControllerUnpublishVolumeResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

func (s *clientResponseValidator) validateVolumeCapabilities(
	ctx context.Context,
	method string,
	rep *csi.ValidateVolumeCapabilitiesResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

func (s *clientResponseValidator) listVolumes(
	ctx context.Context,
	method string,
	rep *csi.ListVolumesResponse) error {

	result := rep.GetResult()
	if result == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	for x, e := range result.Entries {
		volInfo := e.VolumeInfo
		if volInfo == nil {
			return &Error{
				Code:       ErrorNoCode,
				FullMethod: method,
				InnerError: fmt.Errorf(
					"%v: index=%d", ErrNilVolumeInfo.Error(), x),
			}
		}

		handle := volInfo.Handle
		if handle == nil {
			return &Error{
				Code:       ErrorNoCode,
				FullMethod: method,
				InnerError: fmt.Errorf(
					"%v: index=%d", ErrNilVolumeHandle.Error(), x),
			}
		}
		if handle.Id == "" {
			return &Error{
				Code:       ErrorNoCode,
				FullMethod: method,
				InnerError: fmt.Errorf(
					"%v: index=%d", ErrEmptyVolumeID.Error(), x),
			}
		}
		if handle.Metadata != nil && len(handle.Metadata) == 0 {
			return &Error{
				Code:       ErrorNoCode,
				FullMethod: method,
				InnerError: fmt.Errorf(
					"%v: index=%d", ErrNonNilEmptyMetadata.Error(), x),
			}
		}
	}

	return nil
}

func (s *clientResponseValidator) getCapacity(
	ctx context.Context,
	method string,
	rep *csi.GetCapacityResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

func (s *clientResponseValidator) controllerGetCapabilities(
	ctx context.Context,
	method string,
	rep *csi.ControllerGetCapabilitiesResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
//                       CLIENT RESPONSE - IDENTITY                           //
////////////////////////////////////////////////////////////////////////////////

func (s *clientResponseValidator) getSupportedVersions(
	ctx context.Context,
	method string,
	rep *csi.GetSupportedVersionsResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

func (s *clientResponseValidator) getPluginInfo(
	ctx context.Context,
	method string,
	rep *csi.GetPluginInfoResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
//                        CLIENT RESPONSE - NODE                              //
////////////////////////////////////////////////////////////////////////////////

func (s *clientResponseValidator) nodePublishVolume(
	ctx context.Context,
	method string,
	rep *csi.NodePublishVolumeResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

func (s *clientResponseValidator) nodeUnpublishVolume(
	ctx context.Context,
	method string,
	rep *csi.NodeUnpublishVolumeResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

func (s *clientResponseValidator) getNodeID(
	ctx context.Context,
	method string,
	rep *csi.GetNodeIDResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	if rep.GetResult().NodeId == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilNodeID,
		}
	}

	return nil
}

func (s *clientResponseValidator) probeNode(
	ctx context.Context,
	method string,
	rep *csi.ProbeNodeResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}

func (s *clientResponseValidator) nodeGetCapabilities(
	ctx context.Context,
	method string,
	rep *csi.NodeGetCapabilitiesResponse) error {

	if rep.GetResult() == nil {
		return &Error{
			Code:       ErrorNoCode,
			FullMethod: method,
			InnerError: ErrNilResult,
		}
	}

	return nil
}
