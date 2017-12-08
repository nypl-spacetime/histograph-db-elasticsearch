module.exports = {
  settings: {
    number_of_shards: 5,
    number_of_replicas: 0,
    analysis: {
      analyzer: {
        lowercase: {
          type: 'custom',
          filter: 'lowercase',
          tokenizer: 'keyword'
        }
      }
    }
  },
  mappings: {
    _default_: {
      dynamic: false,
      properties: {
        northWest: {
          type: 'geo_point'
        },
        southEast: {
          type: 'geo_point'
        },
        centroid: {
          type: 'geo_point'
        },
        type: {
          type: 'string',
          index: 'not_analyzed'
        },
        name: {
          fields: {
            analyzed: {
              index: 'analyzed',
              type: 'string'
            },
            exact: {
              analyzer: 'lowercase',
              type: 'string'
            }
          },
          type: 'string'
        },
        validSince: {
          type: 'date',
          format: 'date_optional_time'
        },
        validUntil: {
          type: 'date',
          format: 'date_optional_time'
        },
        data: {
          dynamic: false,
          type: 'nested',
          properties: {
            objects: {
              dynamic: false,
              type: 'nested',
              properties: {
                id: {
                  type: 'string',
                  index: 'not_analyzed'
                },
                dataset: {
                  type: 'string',
                  index: 'not_analyzed'
                }
              }
            }
          }
        }
      }
    }
  }
}
