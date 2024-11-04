//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package multimodal

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// Helper methods
// get image and video blob fns
func getBlob(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	content, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(content), nil
}

func GetImageBlob(dataFolderPath string, i int) (string, error) {
	path := fmt.Sprintf("%s/images/%v.jpg", dataFolderPath, i)
	return getBlob(path)
}

func GetVideoBlob(dataFolderPath string, i int) (string, error) {
	path := fmt.Sprintf("%s/videos/%v.mp4", dataFolderPath, i)
	return getBlob(path)
}

func GetCSV(dataFolderPath string) (*os.File, error) {
	return os.Open(fmt.Sprintf("%s/data.csv", dataFolderPath))
}

// query test helper
func TestQuery(t *testing.T,
	className, nearMediaArgument, titleProperty, titlePropertyValue string,
	targetVectors map[string]int,
) {
	var targetVectorsList []string
	for targetVector := range targetVectors {
		targetVectorsList = append(targetVectorsList, targetVector)
	}
	query := fmt.Sprintf(`
			{
				Get {
					%s(
						%s
					){
						%s
						_additional {
							certainty
							vectors {%s}
						}
					}
				}
			}
		`, className, nearMediaArgument, titleProperty, strings.Join(targetVectorsList, ","))

	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	objs := result.Get("Get", className).AsSlice()
	require.Len(t, objs, 2)
	title := objs[0].(map[string]interface{})[titleProperty]
	assert.Equal(t, titlePropertyValue, title)
	additional, ok := objs[0].(map[string]interface{})["_additional"].(map[string]interface{})
	require.True(t, ok)
	certainty := additional["certainty"].(json.Number)
	assert.NotNil(t, certainty)
	certaintyValue, err := certainty.Float64()
	require.NoError(t, err)
	assert.Greater(t, certaintyValue, 0.0)
	assert.GreaterOrEqual(t, certaintyValue, 0.9)
	vectors, ok := additional["vectors"].(map[string]interface{})
	require.True(t, ok)

	targetVectorsMap := make(map[string][]float32)
	for targetVector := range targetVectors {
		vector, ok := vectors[targetVector].([]interface{})
		require.True(t, ok)

		vec := make([]float32, len(vector))
		for i := range vector {
			val, err := vector[i].(json.Number).Float64()
			require.NoError(t, err)
			vec[i] = float32(val)
		}

		targetVectorsMap[targetVector] = vec
	}
	for targetVector, targetVectorDimensions := range targetVectors {
		require.Len(t, targetVectorsMap[targetVector], targetVectorDimensions)
	}
}