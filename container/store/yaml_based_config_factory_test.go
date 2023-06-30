package store

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestYamlBasedPipelineConfigFactory_CreatePipelineConfiguration(t *testing.T) {
	// given
	path, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	file, err := os.Open(filepath.Dir(path) + "/../resources/samplePipelines/tailFileToHttp/pipeline.yaml")
	if err != nil {
		t.Error(err)
	}

	defer util.CloseFile(file)

	config := common.PipelineConfiguration{}

	// when
	err = YamlFileBasedPipelineConfigFactory{file: file}.CreatePipelineConfiguration(&config)
	if err != nil {
		t.Error(err)
	}

	// then
	assert.Equal(t, "tailFileToHttp", config.PipelineId)
	assert.Equal(t, "FileTail_01", config.Stages[0].InstanceName)
	assert.Equal(t, "com_streamsets_pipeline_stage_origin_logtail_FileTailDSource", config.Stages[0].StageName)

	assert.Equal(t, "Trash_01", config.Stages[1].InstanceName)
	assert.Equal(t, "com_streamsets_pipeline_stage_destination_devnull_NullDTarget", config.Stages[1].StageName)

	assert.Equal(t, "HttpClient_01", config.Stages[2].InstanceName)
	assert.Equal(t, "com_streamsets_pipeline_stage_destination_http_HttpClientDTarget", config.Stages[2].StageName)
}
