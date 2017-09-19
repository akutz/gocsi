package gocsi_test

import (
	"fmt"
	"os"

	"github.com/codedellemc/gocsi"
	"github.com/codedellemc/gocsi/csi"
)

var _ = Describe("ParseVersion", func() {
	shouldParse := func() gocsi.Version {
		v, err := gocsi.ParseVersion(
			CurrentGinkgoTestDescription().ComponentTexts[1])
		Ω(err).ShouldNot(HaveOccurred())
		Ω(v).ShouldNot(BeNil())
		return v
	}
	Context("0.0.0", func() {
		It("Should Parse", func() {
			v := shouldParse()
			Ω(v.GetMajor()).Should(Equal(uint32(0)))
			Ω(v.GetMinor()).Should(Equal(uint32(0)))
			Ω(v.GetPatch()).Should(Equal(uint32(0)))
		})
	})
	Context("0.1.0", func() {
		It("Should Parse", func() {
			v := shouldParse()
			Ω(v.GetMajor()).Should(Equal(uint32(0)))
			Ω(v.GetMinor()).Should(Equal(uint32(1)))
			Ω(v.GetPatch()).Should(Equal(uint32(0)))
		})
	})
	Context("1.1.0", func() {
		It("Should Parse", func() {
			v := shouldParse()
			Ω(v.GetMajor()).Should(Equal(uint32(1)))
			Ω(v.GetMinor()).Should(Equal(uint32(1)))
			Ω(v.GetPatch()).Should(Equal(uint32(0)))
		})
	})
})

var _ = Describe("GetCSIEndpoint", func() {
	var (
		err         error
		proto       string
		addr        string
		expEndpoint string
		expProto    string
		expAddr     string
	)
	BeforeEach(func() {
		expEndpoint = CurrentGinkgoTestDescription().ComponentTexts[2]
		os.Setenv(gocsi.CSIEndpoint, expEndpoint)
	})
	AfterEach(func() {
		proto = ""
		addr = ""
		expEndpoint = ""
		expProto = ""
		expAddr = ""
		os.Unsetenv(gocsi.CSIEndpoint)
	})
	JustBeforeEach(func() {
		proto, addr, err = gocsi.GetCSIEndpoint()
	})

	Context("Valid Endpoint", func() {
		shouldBeValid := func() {
			Ω(os.Getenv(gocsi.CSIEndpoint)).Should(Equal(expEndpoint))
			Ω(proto).Should(Equal(expProto))
			Ω(addr).Should(Equal(expAddr))
		}
		Context("tcp://127.0.0.1", func() {
			BeforeEach(func() {
				expProto = "tcp"
				expAddr = "127.0.0.1"
			})
			It("Should Be Valid", shouldBeValid)
		})
		Context("tcp://127.0.0.1:8080", func() {
			BeforeEach(func() {
				expProto = "tcp"
				expAddr = "127.0.0.1:8080"
			})
			It("Should Be Valid", shouldBeValid)
		})
		Context("tcp://*:8080", func() {
			BeforeEach(func() {
				expProto = "tcp"
				expAddr = "*:8080"
			})
			It("Should Be Valid", shouldBeValid)
		})
		Context("unix://path/to/sock.sock", func() {
			BeforeEach(func() {
				expProto = "unix"
				expAddr = "path/to/sock.sock"
			})
			It("Should Be Valid", shouldBeValid)
		})
		Context("unix:///path/to/sock.sock", func() {
			BeforeEach(func() {
				expProto = "unix"
				expAddr = "/path/to/sock.sock"
			})
			It("Should Be Valid", shouldBeValid)
		})
		Context("sock.sock", func() {
			BeforeEach(func() {
				expProto = "unix"
				expAddr = "sock.sock"
			})
			It("Should Be Valid", shouldBeValid)
		})
		Context("/tmp/sock.sock", func() {
			BeforeEach(func() {
				expProto = "unix"
				expAddr = "/tmp/sock.sock"
			})
			It("Should Be Valid", shouldBeValid)
		})
	})

	Context("Missing Endpoint", func() {
		Context("", func() {
			It("Should Be Missing", func() {
				Ω(err).Should(HaveOccurred())
				Ω(err).Should(Equal(gocsi.ErrMissingCSIEndpoint))
			})
		})
		Context("    ", func() {
			It("Should Be Missing", func() {
				Ω(err).Should(HaveOccurred())
				Ω(err).Should(Equal(gocsi.ErrMissingCSIEndpoint))
			})
		})
	})

	Context("Invalid Network Address", func() {
		shouldBeInvalid := func() {
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal(fmt.Sprintf(
				"invalid network address: %s", expEndpoint)))
		}
		Context("tcp5://localhost:5000", func() {
			It("Should Be An Invalid Endpoint", shouldBeInvalid)
		})
		Context("unixpcket://path/to/sock.sock", func() {
			It("Should Be An Invalid Endpoint", shouldBeInvalid)
		})
	})

	Context("Invalid Implied Sock File", func() {
		shouldBeInvalid := func() {
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal(fmt.Sprintf(
				"invalid implied sock file: %[1]s: "+
					"open %[1]s: no such file or directory",
				expEndpoint)))
		}
		Context("Xtcp5://localhost:5000", func() {
			It("Should Be An Invalid Implied Sock File", shouldBeInvalid)
		})
		Context("Xunixpcket://path/to/sock.sock", func() {
			It("Should Be An Invalid Implied Sock File", shouldBeInvalid)
		})
	})
})

var _ = Describe("ParseProtoAddr", func() {
	Context("Empty Address", func() {
		It("Should Be An Empty Address", func() {
			_, _, err := gocsi.ParseProtoAddr("")
			Ω(err).Should(HaveOccurred())
			Ω(err).Should(Equal(gocsi.ErrParseProtoAddrRequired))
		})
		It("Should Be An Empty Address", func() {
			_, _, err := gocsi.ParseProtoAddr("   ")
			Ω(err).Should(HaveOccurred())
			Ω(err).Should(Equal(gocsi.ErrParseProtoAddrRequired))
		})
	})
})

var _ = Describe("ParseVolumeHandle", func() {
	var (
		err    error
		args   []string
		handle *csi.VolumeHandle
	)
	AfterEach(func() {
		err = nil
		handle = nil
	})
	JustBeforeEach(func() {
		handle, err = gocsi.ParseVolumeHandle(args...)
	})
	shouldBeErrParseVolumeHandleEmptyArgs := func() {
		Ω(err).Should(HaveOccurred())
		Ω(err).Should(Equal(gocsi.ErrParseVolumeHandleEmptyArgs))
		Ω(handle).Should(BeNil())
	}
	Context("Nil Variadic", func() {
		BeforeEach(func() {
			args = nil
		})
		It("Should Be ErrParseVolumeHandleEmptyArgs",
			shouldBeErrParseVolumeHandleEmptyArgs)
	})
	Context("Empty Variadic", func() {
		BeforeEach(func() {
			args = []string{}
		})
		It("Should Be ErrParseVolumeHandleEmptyArgs",
			shouldBeErrParseVolumeHandleEmptyArgs)
	})
	Context("ID Only", func() {
		BeforeEach(func() {
			args = []string{"localhost"}
		})
		It("Should Be A Valid VolumeHandle", func() {
			Ω(err).ShouldNot(HaveOccurred())
			Ω(handle).ShouldNot(BeNil())
			Ω(handle.Id).Should(Equal("localhost"))
			Ω(handle.Metadata).Should(BeNil())
		})
	})
	Context("ID & Single Key=Value Pair", func() {
		BeforeEach(func() {
			args = []string{"localhost", "ip=127.0.0.1"}
		})
		It("Should Be A Valid VolumeHandle", func() {
			Ω(err).ShouldNot(HaveOccurred())
			Ω(handle).ShouldNot(BeNil())
			Ω(handle.Id).Should(Equal("localhost"))
			Ω(handle.Metadata).Should(HaveLen(1))
			Ω(handle.Metadata["ip"]).Should(Equal("127.0.0.1"))
		})
	})
	Context("ID & Single Key=Value Pair Sans Key", func() {
		BeforeEach(func() {
			args = []string{"localhost", "online"}
		})
		It("Should Be A Valid VolumeHandle", func() {
			Ω(err).ShouldNot(HaveOccurred())
			Ω(handle).ShouldNot(BeNil())
			Ω(handle.Id).Should(Equal("localhost"))
			Ω(handle.Metadata).Should(HaveLen(1))
			Ω(handle.Metadata["online"]).Should(Equal(""))
		})
	})
	Context("ID & Multiple Key=Value Pairs", func() {
		BeforeEach(func() {
			args = []string{"localhost", "ip=127.0.0.1", "status=online"}
		})
		It("Should Be A Valid VolumeHandle", func() {
			Ω(err).ShouldNot(HaveOccurred())
			Ω(handle).ShouldNot(BeNil())
			Ω(handle.Id).Should(Equal("localhost"))
			Ω(handle.Metadata).Should(HaveLen(2))
			Ω(handle.Metadata["ip"]).Should(Equal("127.0.0.1"))
			Ω(handle.Metadata["status"]).Should(Equal("online"))
		})
	})
})
