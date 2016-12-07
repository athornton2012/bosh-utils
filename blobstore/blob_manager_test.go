package blobstore_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"

	. "github.com/cloudfoundry/bosh-utils/blobstore"
	boshcrypto "github.com/cloudfoundry/bosh-utils/crypto"
	boshcryptofake "github.com/cloudfoundry/bosh-utils/crypto/fakes"
	boshlog "github.com/cloudfoundry/bosh-utils/logger"
	boshsys "github.com/cloudfoundry/bosh-utils/system"
	boshsysfake "github.com/cloudfoundry/bosh-utils/system/fakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Blob Manager", func() {
	var (
		fs           boshsys.FileSystem
		logger       boshlog.Logger
		basePath     string
		blobPath     string
		blobId       string
		toWrite      io.Reader
		digestProvider *boshcryptofake.FakeDigestProvider
	)

	BeforeEach(func() {
		logger = boshlog.NewLogger(boshlog.LevelNone)
		fs = boshsys.NewOsFileSystem(logger)
		blobId = "105d33ae-655c-493d-bf9f-1df5cf3ca847"
		basePath = os.TempDir()
		blobPath = filepath.Join(basePath, blobId)
		toWrite = bytes.NewReader([]byte("new data"))
		digestProvider = &boshcryptofake.FakeDigestProvider{}
	})

	readFile := func(fileIO boshsys.File) []byte {
		fileStat, _ := fileIO.Stat()
		fileBytes := make([]byte, fileStat.Size())
		fileIO.Read(fileBytes)
		return fileBytes
	}

	It("fetches", func() {
		blobManager := NewBlobManager(fs, digestProvider, basePath)
		fs.WriteFileString(blobPath, "some data")

		readOnlyFile, err, _ := blobManager.Fetch(blobId)
		defer fs.RemoveAll(readOnlyFile.Name())

		Expect(err).ToNot(HaveOccurred())
		fileBytes := readFile(readOnlyFile)

		Expect(string(fileBytes)).To(Equal("some data"))
	})

	It("writes", func() {
		blobManager := NewBlobManager(fs, digestProvider, basePath)
		fs.WriteFileString(blobPath, "some data")
		defer fs.RemoveAll(blobPath)

		err := blobManager.Write(blobId, toWrite)
		Expect(err).ToNot(HaveOccurred())

		contents, err := fs.ReadFileString(blobPath)
		Expect(err).ToNot(HaveOccurred())
		Expect(contents).To(Equal("new data"))
	})

	Context("when it writes", func() {
		BeforeEach(func() {
			basePath = filepath.ToSlash(basePath)
			blobPath = filepath.ToSlash(blobPath)
		})

		It("creates and closes the file", func() {
			fs_ := boshsysfake.NewFakeFileSystem()
			blobManager := NewBlobManager(fs_, digestProvider, basePath)
			err := blobManager.Write(blobId, toWrite)
			Expect(err).ToNot(HaveOccurred())
			fileStats, err := fs_.FindFileStats(blobPath)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileStats.Open).To(BeFalse())
		})

		It("creates file with correct permissions", func() {
			fs_ := boshsysfake.NewFakeFileSystem()
			blobManager := NewBlobManager(fs_, digestProvider, basePath)
			err := blobManager.Write(blobId, toWrite)
			fileStats, err := fs_.FindFileStats(blobPath)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileStats.FileMode).To(Equal(os.FileMode(0666)))
			Expect(fileStats.Flags).To(Equal(os.O_WRONLY | os.O_CREATE | os.O_TRUNC))
		})
	})

	Describe("GetPath", func() {
		var sampleDigest boshcrypto.Digest

		BeforeEach(func() {
			blobId = "smurf-24"
			sampleDigest = boshcrypto.NewDigest(boshcrypto.DigestAlgorithmSHA1, "smurf-sha1")
		})

		Context("when file requested does not exist in blobsPath", func() {
			It("returns an error", func() {
				blobManager := NewBlobManager(fs, digestProvider, basePath)

				_, err := blobManager.GetPath("blob-id-does-not-exist", sampleDigest)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal("blob not found"))
			})
		})

		Context("when file requested exists in blobsPath", func() {
			Context("when file checksum matches provided checksum", func() {
				BeforeEach(func() {
					digestProvider.CreateFromFileReturns(boshcrypto.NewDigest("sha1", "smurf-sha1"), nil)
				})

				It("should return the path of a copy of the requested blob", func() {
					blobManager := NewBlobManager(fs, digestProvider, basePath)

					err := fs.WriteFileString(filepath.Join(basePath, blobId), "smurf-content-hello")
					defer fs.RemoveAll(blobPath)

					Expect(err).To(BeNil())

					filename, err := blobManager.GetPath(blobId, sampleDigest)
					Expect(err).To(BeNil())
					Expect(fs.ReadFileString(filename)).To(Equal("smurf-content-hello"))
					Expect(filename).ToNot(Equal(filepath.Join(blobPath, blobId)))
				})
			})

			Context("when file checksum does NOT match provided checksum", func() {
				BeforeEach(func() {
					digestProvider.CreateFromFileReturns(boshcrypto.NewDigest("sha1", "wrong-smurf-sha1"), nil)
				})

				It("should return an error", func() {
					blobManager := NewBlobManager(fs, digestProvider, basePath)

					err := fs.WriteFileString(filepath.Join(basePath, blobId), "smurf-content-hello")
					defer fs.RemoveAll(blobPath)

					Expect(err).To(BeNil())

					filename, err := blobManager.GetPath(blobId, sampleDigest)

					Expect(err).ToNot(BeNil())
					Expect(err.Error()).To(Equal(`Checking blob 'smurf-24': Expected sha1 digest "smurf-sha1" but received "wrong-smurf-sha1"`))

					Expect(filename).To(Equal(""))
				})
			})
		})
	})

	Describe("Delete", func() {
		BeforeEach(func() {
			blobId = "smurf-25"
		})

		Context("when file to be deleted does not exist in blobsPath", func() {
			It("does not freak out", func() {
				blobManager := NewBlobManager(fs, digestProvider, basePath)

				err := blobManager.Delete("hello-i-am-no-one")

				Expect(err).To(BeNil())
			})
		})

		Context("when file to be deleted exists in blobsPath", func() {
			It("should delete the blob", func() {
				err := fs.WriteFileString(filepath.Join(basePath, blobId), "smurf-content")
				Expect(err).To(BeNil())
				Expect(fs.FileExists(filepath.Join(basePath, blobId))).To(BeTrue())

				blobManager := NewBlobManager(fs, digestProvider, basePath)
				err = blobManager.Delete(blobId)
				Expect(err).To(BeNil())

				Expect(fs.FileExists(filepath.Join(basePath, blobId))).To(BeFalse())
			})
		})
	})

	Describe("BlobExists", func() {
		BeforeEach(func() {
			blobId = "super-smurf"
		})

		Context("when blob requested exists in blobsPath", func() {
			It("returns true", func() {
				blobManager := NewBlobManager(fs, digestProvider, basePath)

				err := fs.WriteFileString(filepath.Join(basePath, blobId), "super-smurf-content")
				defer fs.RemoveAll(blobPath)

				Expect(err).To(BeNil())

				exists := blobManager.BlobExists(blobId)
				Expect(exists).To(BeTrue())
			})
		})

		Context("when blob requested does NOT exist in blobsPath", func() {
			It("returns false", func() {
				blobManager := NewBlobManager(fs, digestProvider, basePath)
				exists := blobManager.BlobExists("blob-id-does-not-exist")

				Expect(exists).To(BeFalse())
			})
		})
	})
})
