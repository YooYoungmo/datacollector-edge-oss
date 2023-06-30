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
	Id     string                   `yaml:"id"`
	Stages []ymlPipelineConfigStage `yaml:"stages"`
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

	stages := make([]*common.StageConfiguration, 0)
	for _, stage := range ymlConfigFile.Stages {
		switch stageName := stage.StageName; stageName {
		case YmlPipelineFileTailStageName:
			stages = append(stages, factory.createFileTailStageConfiguration(stage))
		case YmlPipelineTrashStageName:
			stages = append(stages, factory.createTrashConfiguration(stage))
		case YmlPipelineHttpClientStageName:
			stages = append(stages, factory.createHttpClientConfiguration(stage))
		default:
			return errors.New("unsupported stage : " + stageName)
		}
	}

	pipelineConfiguration.Stages = stages
	pipelineConfiguration.ErrorStage = &common.StageConfiguration{
		InstanceName: "Discard_ErrorStage",
		Library:      trash.LIBRARY,
		StageName:    "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget",
	}

	return nil
}

func (factory YamlFileBasedPipelineConfigFactory) createFileTailStageConfiguration(stage ymlPipelineConfigStage) *common.StageConfiguration {
	return &common.StageConfiguration{
		InstanceName: stage.InstanceName,
		Library:      filetail.Library,
		StageName:    filetail.StageName,
		Configuration: []common.Config{
			{
				Name:  "conf.dataFormatConfig.compression",
				Value: "NONE",
			},
			{
				Name:  "conf.dataFormatConfig.filePatternInArchive",
				Value: "*",
			},
			{
				Name:  "conf.dataFormatConfig.charset",
				Value: "UTF-8",
			},
			{
				Name:  "conf.dataFormatConfig.removeCtrlChars",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.textMaxLineLen",
				Value: 1024,
			},
			{
				Name:  "conf.dataFormatConfig.useCustomDelimiter",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.customDelimiter",
				Value: "\\r\\n",
			},
			{
				Name:  "conf.dataFormatConfig.includeCustomDelimiterInTheText",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.jsonContent",
				Value: "MULTIPLE_OBJECTS",
			},
			{
				Name:  "conf.dataFormatConfig.jsonMaxObjectLen",
				Value: 4096,
			},
			{
				Name:  "conf.dataFormatConfig.csvFileFormat",
				Value: "CSV",
			},
			{
				Name:  "conf.dataFormatConfig.csvHeader",
				Value: "NO_HEADER",
			},
			{
				Name:  "conf.dataFormatConfig.csvMaxObjectLen",
				Value: 1024,
			},
			{
				Name:  "conf.dataFormatConfig.csvCustomDelimiter",
				Value: "|",
			},
			{
				Name:  "conf.dataFormatConfig.csvCustomEscape",
				Value: "\\",
			},
			{
				Name:  "conf.dataFormatConfig.csvCustomQuote",
				Value: "\"",
			},
			{
				Name:  "conf.dataFormatConfig.csvEnableComments",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.csvCommentMarker",
				Value: "#",
			},
			{
				Name:  "conf.dataFormatConfig.csvIgnoreEmptyLines",
				Value: true,
			},
			{
				Name:  "conf.dataFormatConfig.csvRecordType",
				Value: "LIST_MAP",
			},
			{
				Name:  "conf.dataFormatConfig.csvSkipStartLines",
				Value: 0,
			},
			{
				Name:  "conf.dataFormatConfig.parseNull",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.nullConstant",
				Value: "\\\\N",
			},
			{
				Name:  "conf.dataFormatConfig.xmlRecordElement",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.includeFieldXpathAttributes",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.xPathNamespaceContext",
				Value: []any{},
			},
			{
				Name:  "conf.dataFormatConfig.outputFieldAttributes",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.xmlMaxObjectLen",
				Value: 4096,
			},
			{
				Name:  "conf.dataFormatConfig.logMode",
				Value: "COMMON_LOG_FORMAT",
			},
			{
				Name:  "conf.dataFormatConfig.logMaxObjectLen",
				Value: 1024,
			},
			{
				Name:  "conf.dataFormatConfig.retainOriginalLine",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.customLogFormat",
				Value: "%h %l %u %t \"%r\" %\u003es %b",
			},
			{
				Name:  "conf.dataFormatConfig.regex",
				Value: "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)",
			},
			{
				Name: "conf.dataFormatConfig.fieldPathsToGroupName",
				Value: []any{
					map[string]any{
						"fieldPath": "/",
						"group":     1,
					},
				},
			},
			{
				Name:  "conf.dataFormatConfig.grokPatternDefinition",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.grokPattern",
				Value: "%{COMMONAPACHELOG}",
			},
			{
				Name:  "conf.dataFormatConfig.onParseError",
				Value: "ERROR",
			},
			{
				Name:  "conf.dataFormatConfig.maxStackTraceLines",
				Value: 50,
			},
			{
				Name:  "conf.dataFormatConfig.enableLog4jCustomLogFormat",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.log4jCustomLogFormat",
				Value: "%r [%t] %-5p %c %x - %m%n",
			},
			{
				Name:  "conf.dataFormatConfig.avroSchemaSource",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.avroSchema",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.schemaRegistryUrls",
				Value: []any{},
			},
			{
				Name:  "conf.dataFormatConfig.schemaLookupMode",
				Value: "SUBJECT",
			},
			{
				Name:  "conf.dataFormatConfig.subject",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.schemaId",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.protoDescriptorFile",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.messageType",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.isDelimited",
				Value: true,
			},
			{
				Name:  "conf.dataFormatConfig.binaryMaxObjectLen",
				Value: 1024,
			},
			{
				Name:  "conf.dataFormatConfig.datagramMode",
				Value: "SYSLOG",
			},
			{
				Name:  "conf.dataFormatConfig.typesDbPath",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.convertTime",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.excludeInterval",
				Value: true,
			},
			{
				Name:  "conf.dataFormatConfig.authFilePath",
				Value: nil,
			},
			{
				Name:  "conf.dataFormatConfig.wholeFileMaxObjectLen",
				Value: 8192,
			},
			{
				Name:  "conf.dataFormatConfig.rateLimit",
				Value: "-1",
			},
			{
				Name:  "conf.dataFormatConfig.verifyChecksum",
				Value: false,
			},
			{
				Name:  "conf.multiLineMainPattern",
				Value: nil,
			},
			{
				Name:  "conf.dataFormat",
				Value: "TEXT",
			},

			{
				Name:  "conf.dataFormat",
				Value: "TEXT",
			},
			{
				Name:  "conf.batchSize",
				Value: "1000",
			},
			{
				Name:  "conf.maxWaitTimeSecs",
				Value: 5,
			},
			{
				Name: "conf.fileInfos",
				Value: []any{
					map[string]any{
						"fileFullPath":    stage.Configuration["filePath"],
						"fileRollMode":    "REVERSE_COUNTER",
						"patternForToken": ".*",
					},
				},
			},
			{
				Name:  "conf.allowLateDirectories",
				Value: false,
			},
			{
				Name:  "conf.postProcessing",
				Value: "NONE",
			},
			{
				Name:  "conf.archiveDir",
				Value: nil,
			},
			{
				Name:  "stageOnRecordError",
				Value: "TO_ERROR",
			},
			{
				Name:  "conf.dataFormatConfig.csvAllowExtraColumns",
				Value: false,
			},
			{
				Name:  "conf.dataFormatConfig.csvExtraColumnPrefix",
				Value: "_extra_",
			},
			{
				Name:  "conf.dataFormatConfig.netflowOutputValuesMode",
				Value: "RAW_AND_INTERPRETED",
			},
			{
				Name:  "conf.dataFormatConfig.maxTemplateCacheSize",
				Value: -1,
			},
			{
				Name:  "conf.dataFormatConfig.templateCacheTimeoutMs",
				Value: -1,
			},
			{
				Name:  "conf.dataFormatConfig.netflowOutputValuesModeDatagram",
				Value: "RAW_AND_INTERPRETED",
			},
			{
				Name:  "conf.dataFormatConfig.maxTemplateCacheSizeDatagram",
				Value: -1,
			},
			{
				Name:  "conf.dataFormatConfig.templateCacheTimeoutMsDatagram",
				Value: -1,
			},
		},
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
	return &common.StageConfiguration{
		InstanceName: stage.InstanceName,
		Library:      http.Library,
		StageName:    http.StageName,
		Configuration: []common.Config{
			{
				Name:  "conf.dataGeneratorFormatConfig.charset",
				Value: "UTF-8",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.csvFileFormat",
				Value: "CSV",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.csvHeader",
				Value: "NO_HEADER",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.csvReplaceNewLines",
				Value: true,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.csvReplaceNewLinesString",
				Value: " ",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.csvCustomDelimiter",
				Value: "|",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.csvCustomEscape",
				Value: "\\",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.csvCustomQuote",
				Value: "\"",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.jsonMode",
				Value: "MULTIPLE_OBJECTS",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.textFieldPath",
				Value: "/text",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.textRecordSeparator",
				Value: "\\n",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.textFieldMissingAction",
				Value: "ERROR",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.textEmptyLineIfNull",
				Value: false,
			},

			{
				Name:  "conf.dataFormat",
				Value: "TEXT",
			},
			{
				Name:  "conf.resourceUrl",
				Value: stage.Configuration["httpUrl"],
			},
			{
				Name: "conf.headers",
				Value: []any{
					map[string]any{
						"key":   "X-SDC-APPLICATION-ID",
						"value": "1234", // Hostname 을 가져와서 찍자.
					},
				},
			},
			{
				Name:  "conf.httpMethod",
				Value: "POST",
			},
			{
				Name:  "conf.client.httpCompression",
				Value: "NONE",
			},
			{
				Name:  "conf.client.connectTimeoutMillis",
				Value: 0,
			},
			{
				Name:  "conf.client.readTimeoutMillis",
				Value: 0,
			},
			{
				Name:  "conf.client.numThreads",
				Value: 1,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.avroSchemaSource",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.avroSchema",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.registerSchema",
				Value: false,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.schemaRegistryUrlsForRegistration",
				Value: []any{},
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.schemaRegistryUrls",
				Value: []any{},
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.schemaLookupMode",
				Value: "SUBJECT",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.subject",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.subjectToRegister",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.schemaId",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.includeSchema",
				Value: true,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.avroCompression",
				Value: "NULL",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.binaryFieldPath",
				Value: "/",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.protoDescriptorFile",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.messageType",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.fileNameEL",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.wholeFileExistsAction",
				Value: "TO_ERROR",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.includeChecksumInTheEvents",
				Value: nil,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.checksumAlgorithm",
				Value: "MD5",
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.xmlPrettyPrint",
				Value: true,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.xmlValidateSchema",
				Value: false,
			},
			{
				Name:  "conf.dataGeneratorFormatConfig.xmlSchema",
				Value: nil,
			},
			{
				Name:  "conf.client.authType",
				Value: "NONE",
			},
			{
				Name:  "conf.client.useOAuth2",
				Value: false,
			},
			{
				Name:  "conf.client.oauth.consumerKey",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth.consumerSecret",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth.token",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth.tokenSecret",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.credentialsGrantType",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.tokenUrl",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.clientId",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.clientSecret",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.username",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.password",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.resourceOwnerClientId",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.resourceOwnerClientSecret",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.algorithm",
				Value: "NONE",
			},
			{
				Name:  "conf.client.oauth2.key",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.jwtClaims",
				Value: nil,
			},
			{
				Name:  "conf.client.oauth2.transferEncoding",
				Value: "BUFFERED",
			},
			{
				Name:  "conf.client.oauth2.additionalValues",
				Value: []any{},
			},
			{
				Name:  "conf.client.basicAuth.username",
				Value: nil,
			},
			{
				Name:  "conf.client.basicAuth.password",
				Value: nil,
			},
			{
				Name:  "conf.client.useProxy",
				Value: false,
			},
			{
				Name:  "conf.client.proxy.uri",
				Value: nil,
			},
			{
				Name:  "conf.client.proxy.username",
				Value: nil,
			},
			{
				Name:  "conf.client.proxy.password",
				Value: nil,
			},
			{
				Name:  "conf.client.tlsConfig.tlsEnabled",
				Value: false,
			},
			{
				Name:  "conf.client.tlsConfig.keyStoreFilePath",
				Value: nil,
			},
			{
				Name:  "conf.client.tlsConfig.keyStoreType",
				Value: "JKS",
			},
			{
				Name:  "conf.client.tlsConfig.keyStorePassword",
				Value: nil,
			},
			{
				Name:  "conf.client.tlsConfig.keyStoreAlgorithm",
				Value: "SunX509",
			},
			{
				Name:  "conf.client.tlsConfig.trustStoreFilePath",
				Value: nil,
			},
			{
				Name:  "conf.client.tlsConfig.trustStoreType",
				Value: "JKS",
			},
			{
				Name:  "conf.client.tlsConfig.trustStorePassword",
				Value: nil,
			},
			{
				Name:  "conf.client.tlsConfig.trustStoreAlgorithm",
				Value: "SunX509",
			},
			{
				Name:  "conf.client.tlsConfig.useDefaultProtocols",
				Value: true,
			},
			{
				Name:  "conf.client.tlsConfig.protocols",
				Value: []any{},
			},
			{
				Name:  "conf.client.tlsConfig.useDefaultCiperSuites",
				Value: true,
			},
			{
				Name:  "conf.client.tlsConfig.cipherSuites",
				Value: []any{},
			},
			{
				Name:  "conf.singleRequestPerBatch",
				Value: true,
			},
			{
				Name:  "conf.rateLimit",
				Value: 0,
			},
			{
				Name:  "conf.maxRequestCompletionSecs",
				Value: 60,
			},
			{
				Name:  "stageOnRecordError",
				Value: "TO_ERROR",
			},
			{
				Name:  "stageRequiredFields",
				Value: []any{},
			},
			{
				Name:  "stageRecordPreconditions",
				Value: []any{},
			},
			{
				Name:  "conf.client.requestLoggingConfig.enableRequestLogging",
				Value: false,
			},
			{
				Name:  "conf.client.requestLoggingConfig.logLevel",
				Value: "FINE",
			},
			{
				Name:  "conf.client.requestLoggingConfig.verbosity",
				Value: "HEADERS_ONLY",
			},
			{
				Name:  "conf.client.requestLoggingConfig.maxEntitySize",
				Value: 0,
			},
		},
		UiInfo: map[string]any{
			"stageType": creation.TARGET,
		},
		InputLanes: []string{"FileTail_01OutputLane14921104146950"},
	}
}
