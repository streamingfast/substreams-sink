package sink

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadManifestAndModule(t *testing.T) {
	type args struct {
		manifestPath             string
		outputModuleName         string
		expectedOutputModuleType string
	}
	tests := []struct {
		name                 string
		args                 args
		wantPackagePresent   bool
		wantModuleName       string
		wantOutputModuleHash string
		assertion            assert.ErrorAssertionFunc
	}{
		{"default", args{"testdata/substreams.yaml", "kv_out", "kv-out"}, true, "kv_out", "f0b74c6dc57fa840bf1e7ff526431f9f1b5240d0", assert.NoError},
		{"multile expected type accepted", args{"testdata/substreams.yaml", "kv_out", "kv-out,graph-out"}, true, "kv_out", "f0b74c6dc57fa840bf1e7ff526431f9f1b5240d0", assert.NoError},
		{"multile expected type accepted, inverted", args{"testdata/substreams.yaml", "graph_out", "kv-out,graph-out"}, true, "graph_out", "54acb6611a4a4b430c81e66639159efb49b618d5", assert.NoError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPkg, gotModule, gotOutputModuleHash, err := ReadManifestAndModule(tt.args.manifestPath, tt.args.outputModuleName, tt.args.expectedOutputModuleType, zlog)
			tt.assertion(t, err)

			if tt.wantPackagePresent {
				assert.NotNil(t, gotPkg)
			} else {
				assert.Nil(t, gotPkg)
			}

			assert.Equal(t, tt.wantModuleName, gotModule.Name)
			assert.Equal(t, tt.wantOutputModuleHash, hex.EncodeToString(gotOutputModuleHash))
		})
	}
}
