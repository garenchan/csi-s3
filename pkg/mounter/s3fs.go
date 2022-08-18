package mounter

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/golang/glog"
	"k8s.io/utils/mount"

	"github.com/ctrox/csi-s3/pkg/common"
	"github.com/ctrox/csi-s3/pkg/s3"
)

// Implements Mounter
type s3fsMounter struct {
	meta          *s3.FSMeta
	url           string
	region        string
	pwFileContent string
}

const (
	s3fsCmd = "s3fs"
)

func newS3fsMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	return &s3fsMounter{
		meta:          meta,
		url:           cfg.Endpoint,
		region:        cfg.Region,
		pwFileContent: cfg.AccessKeyID + ":" + cfg.SecretAccessKey,
	}, nil
}

func (s3fs *s3fsMounter) Stage(stageTarget string) error {
	if err := writes3fsPass(s3fs.pwFileContent); err != nil {
		return err
	}

	args := []string{
		fmt.Sprintf("%s:/%s", s3fs.meta.BucketName, path.Join(s3fs.meta.Prefix, s3fs.meta.FSPath)),
		stageTarget,
		"-o", "use_path_request_style",
		"-o", fmt.Sprintf("url=%s", s3fs.url),
		"-o", fmt.Sprintf("endpoint=%s", s3fs.region),
		"-o", "allow_other",
		"-o", "mp_umask=000",
	}

	// parse and append extra options
	extraOptions := s3fs.parseExtraOptions()
	args = append(args, extraOptions...)

	return fuseMount(stageTarget, s3fsCmd, args)
}

func (s3fs *s3fsMounter) parseExtraOptions() []string {
	extraOptions := s3fs.meta.ExtraOptions

	// We support the use of placeholders in options, such as ${cacheDir}
	extraOptions = os.Expand(extraOptions, s3fs.extraOptionsMapping)

	return strings.Fields(extraOptions)
}

func (s3fs *s3fsMounter) extraOptionsMapping(name string) string {
	switch name {
	case "cacheDir":
		return getCacheDir(s3fs.meta.BucketName)
	}

	glog.Warningf("Unknown extra option placeholder %q of bucket %s", name, s3fs.meta.BucketName)
	return name
}

func (s3fs *s3fsMounter) Unstage(stageTarget string) error {
	if err := FuseUnmount(stageTarget); err != nil {
		return err
	}

	if err := os.Remove(stageTarget); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (s3fs *s3fsMounter) Mount(source string, target string) error {
	mounter := mount.New("")
	// Use bind mount to create an alias of the real mount point.
	mountOptions := []string{"bind"}

	if err := mounter.Mount(source, target, "", mountOptions); err != nil {
		return err
	}

	return nil
}

func (s3fs *s3fsMounter) Unmount(target string) error {
	return common.CleanupMountPoint(target)
}

func writes3fsPass(pwFileContent string) error {
	pwFileName := fmt.Sprintf("%s/.passwd-s3fs", os.Getenv("HOME"))
	pwFile, err := os.OpenFile(pwFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	_, err = pwFile.WriteString(pwFileContent)
	if err != nil {
		return err
	}
	pwFile.Close()
	return nil
}
