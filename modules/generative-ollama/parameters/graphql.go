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

package parameters

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func input(prefix string) *graphql.InputObjectFieldConfig {
	return &graphql.InputObjectFieldConfig{
		Description: fmt.Sprintf("%s settings", Name),
		Type: graphql.NewInputObject(graphql.InputObjectConfig{
			Name: fmt.Sprintf("%s%sInputObject", prefix, Name),
			Fields: graphql.InputObjectConfigFieldMap{
				"apiEndpoint": &graphql.InputObjectFieldConfig{
					Description: "apiEndpoint",
					Type:        graphql.String,
				},
				"model": &graphql.InputObjectFieldConfig{
					Description: "model",
					Type:        graphql.String,
				},
				"temperature": &graphql.InputObjectFieldConfig{
					Description: "temperature",
					Type:        graphql.Float,
				},
			},
		}),
		DefaultValue: nil,
	}
}