package store

import (
	"errors"
	uuid "github.com/satori/go.uuid"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/stages/destinations/http"
	"github.com/streamsets/datacollector-edge/stages/destinations/trash"
	"github.com/streamsets/datacollector-edge/stages/origins/filetail"
	"gopkg.in/yaml.v3"
	"os"
)

const (
	YmlPipelineFileTailStageName   = "FileTail"
	YmlPipelineTrashStageName      = "Trash"
	YmlPipelineHttpClientStageName = "HttpClient"
)

type ymlPipelineConfigFile struct {
	Id     string             `yaml:"id"`
	Stages yamlPipelineStages `yaml:"stages"`
}

type yamlPipelineStages struct {
	Origin       ymlPipelineConfigStage   `yaml:"origin"`
	Processor    []ymlPipelineConfigStage `yaml:"processors"`
	Destinations []ymlPipelineConfigStage `yaml:"destinations"`
}

type ymlPipelineConfigStage struct {
	InstanceName  string         `yaml:"instanceName"`
	StageName     string         `yaml:"stageName"`
	Configuration map[string]any `yaml:"configuration"`
}

type YamlFileBasedPipelineConfigFactory struct {
	file *os.File
}

// CreatePipelineConfiguration yaml 파일을 읽어서 지정한 내용을 제외하고는 모두 기본 값을 채움.
func (factory YamlFileBasedPipelineConfigFactory) CreatePipelineConfiguration(pipelineConfiguration *common.PipelineConfiguration) error {
	ymlConfigFile := ymlPipelineConfigFile{}
	decoder := yaml.NewDecoder(factory.file)
	err := decoder.Decode(&ymlConfigFile)
	if err != nil {
		return err
	}

	pipelineConfiguration.PipelineId = ymlConfigFile.Id
	pipelineUUID := uuid.NewV4().String()
	pipelineConfiguration.UUID = pipelineUUID
	pipelineConfiguration.Configuration = creation.GetDefaultPipelineConfigs()
	pipelineConfiguration.Info = common.PipelineInfo{
		PipelineId: ymlConfigFile.Id,
		Title:      ymlConfigFile.Id,
		UUID:       pipelineUUID,
		Valid:      true,
	}

	pipelineConfiguration.UiInfo = map[string]interface{}{}
	pipelineConfiguration.ErrorStage = creation.GetTrashErrorStageInstance()
	pipelineConfiguration.StatsAggregatorStage = creation.GetDefaultStatsAggregatorStageInstance()

	stages, err := factory.createStageConfigurations(ymlConfigFile.Stages)
	if err != nil {
		return err
	}

	pipelineConfiguration.Stages = stages
	pipelineConfiguration.ErrorStage = &common.StageConfiguration{
		InstanceName: "Discard_ErrorStage",
		Library:      trash.LIBRARY,
		StageName:    "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget",
	}

	return nil
}

func (factory YamlFileBasedPipelineConfigFactory) createStageConfigurations(stages yamlPipelineStages) ([]*common.StageConfiguration, error) {
	resultStages := make([]*common.StageConfiguration, 0)

	originStageConfig, err := factory.createOriginStageConfig(stages.Origin)
	if err != nil {
		return nil, err
	}
	resultStages = append(resultStages, originStageConfig)

	for _, destinationStage := range stages.Destinations {
		stageConfig, err := factory.createDestinationStageConfig(destinationStage)
		if err != nil {
			return nil, err
		}
		resultStages = append(resultStages, stageConfig)
	}
	return resultStages, nil
}

func (factory YamlFileBasedPipelineConfigFactory) createOriginStageConfig(stage ymlPipelineConfigStage) (*common.StageConfiguration, error) {
	if stage.StageName == YmlPipelineFileTailStageName {
		return factory.createFileTailStageConfiguration(stage), nil
	}
	return nil, errors.New("unsupported stage : " + stage.StageName)
}

func (factory YamlFileBasedPipelineConfigFactory) createDestinationStageConfig(stage ymlPipelineConfigStage) (*common.StageConfiguration, error) {
	if stage.StageName == YmlPipelineTrashStageName || stage.StageName == YmlPipelineHttpClientStageName {
		switch stageName := stage.StageName; stageName {
		case YmlPipelineTrashStageName:
			return factory.createTrashConfiguration(stage), nil
		case YmlPipelineHttpClientStageName:
			return factory.createHttpClientConfiguration(stage), nil
		default:
			return nil, errors.New("unsupported stage : " + stageName)
		}
	}
	return nil, errors.New("unsupported stage : " + stage.StageName)
}

func (factory YamlFileBasedPipelineConfigFactory) createFileTailStageConfiguration(stage ymlPipelineConfigStage) *common.StageConfiguration {
	configs := filetail.GetDefaultStageConfigs()

	for i := 0; i < len(configs); i++ {
		if configs[i].Name == "conf.fileInfos" {
			configs[i].Value = []any{
				map[string]any{
					"fileFullPath":    stage.Configuration["filePath"],
					"fileRollMode":    "REVERSE_COUNTER",
					"patternForToken": ".*",
				},
			}
		}
	}

	return &common.StageConfiguration{
		InstanceName:  stage.InstanceName,
		Library:       filetail.Library,
		StageName:     filetail.StageName,
		Configuration: configs,
		UiInfo: map[string]any{
			"stageType": creation.SOURCE,
			"rawSource": map[string]any{
				"configuration": []any{
					map[string]any{
						"name": "fileName",
					},
				},
			},
		},
		OutputLanes: []string{"FileTail_01OutputLane14921104146950", "FileTail_01OutputLane14921104146951"},
	}
}

func (factory YamlFileBasedPipelineConfigFactory) createTrashConfiguration(stage ymlPipelineConfigStage) *common.StageConfiguration {
	stageName := trash.NULL_STAGE_NAME

	if stage.Configuration["type"] == "ERROR" {
		stageName = trash.ERROR_STAGE_NAME
	} else if stage.Configuration["type"] == "STATS_NULL" {
		stageName = trash.STATS_NULL_STAGE_NAME
	} else if stage.Configuration["type"] == "STATS_DPM_DIRECTLY" {
		stageName = trash.STATS_DPM_DIRECTLY_STAGE_NAME
	}

	return &common.StageConfiguration{
		InstanceName: stage.InstanceName,
		Library:      trash.LIBRARY,
		StageName:    stageName,
		UiInfo: map[string]any{
			"stageType": creation.TARGET,
		},
		InputLanes: []string{"FileTail_01OutputLane14921104146951"},
	}
}

func (factory YamlFileBasedPipelineConfigFactory) createHttpClientConfiguration(stage ymlPipelineConfigStage) *common.StageConfiguration {
	configs := http.GetDefaultStageConfigs()

	for i := 0; i < len(configs); i++ {
		if configs[i].Name == "conf.resourceUrl" {
			configs[i].Value = stage.Configuration["httpUrl"]
		}
	}

	return &common.StageConfiguration{
		InstanceName:  stage.InstanceName,
		Library:       http.Library,
		StageName:     http.StageName,
		Configuration: configs,
		UiInfo: map[string]any{
			"stageType": creation.TARGET,
		},
		InputLanes: []string{"FileTail_01OutputLane14921104146950"},
	}
}
