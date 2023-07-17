// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package http

import (
	"bytes"
	"compress/gzip"
	"errors"
	"net/http"

	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/lib/httpcommon"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	Library   = "streamsets-datacollector-basic-lib"
	StageName = "com_streamsets_pipeline_stage_destination_http_HttpClientDTarget"
)

type HttpClientDestination struct {
	*common.BaseStage
	*httpcommon.HttpCommon
	Conf HttpClientTargetConfig `ConfigDefBean:"conf"`
}

type HttpClientTargetConfig struct {
	ResourceUrl               string                                  `ConfigDef:"type=STRING,required=true"`
	Headers                   map[string]string                       `ConfigDef:"type=MAP,required=true"`
	SingleRequestPerBatch     bool                                    `ConfigDef:"type=BOOLEAN,required=true"`
	Client                    httpcommon.ClientConfigBean             `ConfigDefBean:"client"`
	DataFormat                string                                  `ConfigDef:"type=STRING,required=true"`
	DataGeneratorFormatConfig datagenerator.DataGeneratorFormatConfig `ConfigDefBean:"dataGeneratorFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &HttpClientDestination{BaseStage: &common.BaseStage{}, HttpCommon: &httpcommon.HttpCommon{}}
	})
}

func (h *HttpClientDestination) Init(stageContext api.StageContext) []validation.Issue {
	issues := h.BaseStage.Init(stageContext)
	if err := h.InitializeClient(h.Conf.Client); err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}
	return h.Conf.DataGeneratorFormatConfig.Init(h.Conf.DataFormat, stageContext, issues)
}

func (h *HttpClientDestination) Write(batch api.Batch) error {
	if h.Conf.SingleRequestPerBatch && len(batch.GetRecords()) > 0 {
		return h.writeSingleRequestPerBatch(batch)
	} else {
		return h.writeSingleRequestPerRecord(batch)
	}
}

func (h *HttpClientDestination) writeSingleRequestPerBatch(batch api.Batch) error {
	var err error
	recordWriterFactory := h.Conf.DataGeneratorFormatConfig.RecordWriterFactory
	batchBuffer := bytes.NewBuffer([]byte{})
	recordWriter, err := recordWriterFactory.CreateWriter(h.GetStageContext(), batchBuffer)
	if err != nil {
		log.Error(err.Error())
		h.GetStageContext().ReportError(err)
		return nil
	}
	for _, record := range batch.GetRecords() {
		err = recordWriter.WriteRecord(record)
		if err != nil {
			log.Error(err.Error())
			h.GetStageContext().ToError(err, record)
		}
	}
	_ = recordWriter.Flush()
	_ = recordWriter.Close()
	err = h.sendToSDC(batchBuffer.Bytes())

	if err != nil {
		log.Error(err.Error())
		for _, record := range batch.GetRecords() {
			h.GetStageContext().ToError(err, record)
		}
	}

	return nil
}

func (h *HttpClientDestination) writeSingleRequestPerRecord(batch api.Batch) error {
	recordWriterFactory := h.Conf.DataGeneratorFormatConfig.RecordWriterFactory
	for _, record := range batch.GetRecords() {
		recordBuffer := bytes.NewBuffer([]byte{})
		recordWriter, err := recordWriterFactory.CreateWriter(h.GetStageContext(), recordBuffer)
		if err != nil {
			log.Error(err.Error())
			h.GetStageContext().ReportError(err)
			continue
		}
		err = recordWriter.WriteRecord(record)
		if err != nil {
			log.Error(err.Error())
			h.GetStageContext().ReportError(err)
			continue
		}
		_ = recordWriter.Flush()
		_ = recordWriter.Close()
		err = h.sendToSDC(recordBuffer.Bytes())
		if err != nil {
			log.Error(err.Error())
			h.GetStageContext().ToError(err, record)
		}
	}
	return nil
}

func (h *HttpClientDestination) sendToSDC(jsonValue []byte) error {
	var buf bytes.Buffer

	if h.Conf.Client.HttpCompression == "GZIP" {
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(jsonValue); err != nil {
			return err
		}
		_ = gz.Close()
	} else {
		buf = *bytes.NewBuffer(jsonValue)
	}

	req, err := http.NewRequest("POST", h.Conf.ResourceUrl, &buf)
	if h.Conf.Headers != nil {
		for key, value := range h.Conf.Headers {
			req.Header.Set(key, value)
		}
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	if h.Conf.Client.HttpCompression == "GZIP" {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := h.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.WithField("status", resp.Status).Debug("Response status")
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	return nil
}

func GetDefaultStageConfigs() []common.Config {
	return []common.Config{
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
			Value: "",
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
	}
}
