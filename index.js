'use strict'

const R = require('ramda')
const H = require('highland')
const config = require('spacetime-config')
const elasticsearch = require('elasticsearch')
const fuzzyDates = require('fuzzy-dates')
const turf = {
  centroid: require('@turf/centroid'),
  bbox: require('@turf/bbox')
}

// module.exports.client = function () {

// }

if (!config.elasticsearch || !config.elasticsearch.host || !config.elasticsearch.port) {
  throw new Error('Please specify elasticsearch.host and elasticsearch.port in the NYC Space/Time Directory configuration file')
}

const esClient = new elasticsearch.Client({
  host: config.elasticsearch.host + ':' + config.elasticsearch.port
})

const defaultMapping = require('./default-mapping')

const pageSize = 100

function baseQuery () {
  return {
    size: pageSize,
    query: {
      bool: {
        must: []
      }
    }
  }
}

module.exports.updateAliases = function (indexOld, indexNew, alias, callback) {
  esClient.indices.updateAliases({
    body: {
      actions: [
        {
          remove: {
            index: indexOld,
            alias
          }
        },
        {
          add: {
            index: indexNew,
            alias
          }
        }
      ]
    }
  }, callback)
}

module.exports.putAlias = function (index, alias, callback) {
  esClient.indices.putAlias({
    index,
    name: alias
  }, callback)
}

module.exports.getAliasedIndex = function (alias, callback) {
  esClient.indices.getAlias({
    name: alias
  }, (err, response) => {
    if (err) {
      callback(err)
    } else {
      callback(null, Object.keys(response)[0])
    }
  })
}

module.exports.query = function (params, callback) {
  esClient.search(params)
    .then((resp) => callback(null, resp), callback)
    .catch(callback)
}

module.exports.delete = function (indices, callback) {
  let index = '*'
  if (indices) {
    index = indices.join(',')
  }

  esClient.indices.delete({
    index
  }, callback)
}

module.exports.search = function (params, callback) {
  const onlyIds = false

  if (!params) {
    params = {}
  }

  let index = '*'
  if (params.datasetIds) {
    // index = params.datasetIds.join(',')
  }

  let query = baseQuery()

  if (onlyIds) {
    query._source = [
      '_id'
    ]
  }

  if (params.id) {
    // query.size = 1
    // query.query.bool.must.push({
    //   constant_score: {
    //     filter: {
    //       term: {
    //         // 'data.objects.id': params.id
    //         // 'data.objects.id': 'addresses/154887-1'
    //         'data.objects.dataset': 'addresses'
    //       }
    //     }
    //   }
    // })

    query = {
      query: {
        nested: {
          path: 'data',
          query: {
            nested: {
              path: 'objects',
              query: {
                bool: {
                  must: [
                      { "match" : {"data.object.dataset" : "addresses"} },

                  ]
                }
              }
            }
          }
        }
      }
    }

    // query.query.bool.must.push({
    //   nested: {
    //     path: 'data',
    //     query: {
    //       nested: {
    //         path: 'data.objects',
    //         filter: {
    //           term: {
    //             'data.objects.dataset': 'addresses'
    //           }
    //         }
    //       }
    //     }
    //   }
    // })
  }

  console.log(query)

  if (params.name) {
    const field = 'name.' + (params.exact ? 'exact' : 'analyzed')

    query.query.bool.must.push({
      query_string: {
        query: params.name,
        fields: [
          field
        ]
      }
    })
  }

  // var id = params.uri || params.id
  // if (id) {
  //   query.query.filtered.filter.bool.must.push({
  //     term: {
  //       _id: id
  //     }
  //   })
  // }

  if (params.type) {
    const types = params.type.map((type) => ({
      type: {
        value: type
      }
    }))

    query.query.bool.must.push({
      bool: {
        should: types
      }
    })
  }

  if (params.geometry) {
    query.query.bool.must.push({
      bool: {
        should: [
          {
            geo_bounding_box: {
              northWest: {
                top_left: {
                  lat: params.geometry[0][1],
                  lon: params.geometry[0][0]
                },
                bottom_right: {
                  lat: params.geometry[1][1],
                  lon: params.geometry[1][0]
                }
              }
            }
          },
          {
            geo_bounding_box: {
              southEast: {
                top_left: {
                  lat: params.geometry[0][1],
                  lon: params.geometry[0][0]
                },
                bottom_right: {
                  lat: params.geometry[1][1],
                  lon: params.geometry[1][0]
                }
              }
            }
          }
        ]
      }
    })
  }

  //   query.query.filtered.filter.bool.must.push({
  //     or: [
  //       {
  //         geo_bounding_box: {
  //           northWest: {
  //             top_left: {
  //               lat: params.intersects[0][1],
  //               lon: params.intersects[0][0]
  //             },
  //             bottom_right: {
  //               lat: params.intersects[1][1],
  //               lon: params.intersects[1][0]
  //             }
  //           }
  //         }
  //       },
  //       {
  //         geo_bounding_box: {
  //           southEast: {
  //             top_left: {
  //               lat: params.intersects[0][1],
  //               lon: params.intersects[0][0]
  //             },
  //             bottom_right: {
  //               lat: params.intersects[1][1],
  //               lon: params.intersects[1][0]
  //             }
  //           }
  //         }
  //       }
  //     ]
  //   })
  // }

  if (params.contains) {
    query.query.bool.must.push(
      {
        geo_bounding_box: {
          northWest: {
            top_left: {
              lat: params.contains[0][1],
              lon: params.contains[0][0]
            },
            bottom_right: {
              lat: params.contains[1][1],
              lon: params.contains[1][0]
            }
          }
        }
      },
      {
        geo_bounding_box: {
          southEast: {
            top_left: {
              lat: params.contains[0][1],
              lon: params.contains[0][0]
            },
            bottom_right: {
              lat: params.contains[1][1],
              lon: params.contains[1][0]
            }
          }
        }
      }
    )
  }

  // if (params.before) {
  //   query.query.bool.must.push({
  //     range: {
  //       validSince: {
  //         lte: params.before
  //       }
  //     }
  //   })
  // }

  // if (params.after) {
  //   query.query.bool.must.push({
  //     range: {
  //       validUntil: {
  //         gte: params.after
  //       }
  //     }
  //   })
  // }

  console.log(JSON.stringify(query, null, 2))

  esClient.search({
    index: index,
    body: query
  }).then((response) => {
    // TODO: convert IDs + URIs
    console.log(response)
    callback(null, response.hits.hits.map((hit) => {
      return Object.assign({dataset: hit._index}, hit._source)
    }))
  })
  .catch(callback)
}

const contextTypeToESMapping = {
  'xsd:string': {
    type: 'string'
  },
  'xsd:boolean': {
    type: 'boolean'
  },
  'xsd:date': {
    type: 'date',
    format: 'date_optional_time'
  },
  'xsd:integer': {
    type: 'integer'
  },
  'xsd:double': {
    type: 'double'
  }
}

function getMapping (message) {
  let mapping = Object.assign({}, defaultMapping)

  const context = message.payload.jsonldContext
  if (context) {
    var pairs = Object.keys(context)
      .filter((key) => context[key]['@type'])
      .map((key) => {
        var type = context[key]['@type']
        if (contextTypeToESMapping[type]) {
          return [
            key,
            contextTypeToESMapping[type]
          ]
        } else {
          return null
        }
      })

    const properties = R.fromPairs(pairs)

    mapping.mappings['_default_'].properties.data = {
      type: 'nested',
      include_in_parent: true,
      properties: properties
    }
  }

  return mapping
}

// Curried ES functions
const createIndex = R.curry(esClient.indices.create.bind(esClient.indices))
const deleteIndex = R.curry(esClient.indices.delete.bind(esClient.indices))
const indexFns = {
  create: createIndex,
  delete: deleteIndex
}

function elasticBulk (objectMessages, callback) {
  if (objectMessages.length) {
    let body

    try {
      body = R.flatten(objectMessages.map(toElasticOperation))
    } catch (err) {
      callback(err)
      return
    }

    esClient.bulk({body}, (err, response) => {
      if (!response) {
        response = {
          took: 0,
          errors: false,
          items: []
        }
      }

      const length = (response.items && response.items.length) || 0
      console.log('      Elasticsearch => %d indexed, took %dms, errors: %s', length, response.took, response.errors)
      callback(err)
    })
  } else {
    callback()
  }
}

module.exports.bulk = function (messages, callback) {
  let objectMessages = []
  const objectsToMessage = (objectMessages) => ({
    type: 'objects',
    payload: objectMessages
  })

  H(messages)
    .filter((message) => message.type === 'object' || message.type === 'dataset')
    .consume((err, message, push, next) => {
      if (err) {
        push(err)
        next()
      } else if (message === H.nil) {
        if (objectMessages.length) {
          push(null, objectsToMessage(objectMessages))
        }
        push(null, message)
      } else if (message.type !== 'object') {
        if (objectMessages.length) {
          push(null, objectsToMessage(objectMessages))
        }

        push(null, message)
        next()
      } else {
        objectMessages.push(message)
        next()
      }
    })
    .map((message) => {
      if (message.type === 'objects') {
        const objectMessages = message.payload
        return R.curry(elasticBulk)(objectMessages)
      } else if (message.type === 'dataset') {
        const indexFn = indexFns[message.action]
        if (indexFn) {
          return indexFn({
            index: message.payload.id,
            body: message.action === 'create' ? getMapping(message) : undefined
          })
        }
      }
    })
    .compact()
    .nfcall([])
    .series()
    .errors((err, push) => {
      if (!err.message.startsWith('[index_already_exists_exception]')) {
        push(err)
      }
    })
    .stopOnError(callback)
    .done(callback)
}

// Message action to Elasticsearch operations
const elasticsearchOperations = {
  create: 'index',
  update: 'index',
  delete: 'delete'
}

// Convert message to Elasticsearch bulk operation
function toElasticOperation (message) {
  // select appropriate ES operation
  const operation = elasticsearchOperations[message.action]

  // { "action": { _index ... } , see
  // https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference-2-0.html
  if (message.payload.geometry) {
    // turf.centroid returns a GeoJSON Point Feature, with coordinates in lon, lat order
    const centroid = turf.centroid(message.payload.geometry).geometry.coordinates

    // turf.extent returns bounding box array, in west, south, east, north order
    const bbox = turf.bbox(message.payload.geometry)

    // The Elasticsearch geo_point type expects [lon, lat] arrays
    message.payload.centroid = centroid
    message.payload.northWest = [bbox[0], bbox[3]]
    message.payload.southEast = [bbox[2], bbox[1]]
  }

  if (message.payload.validSince) {
    const validSince = fuzzyDates.convert(message.payload.validSince)[0]
    message.payload.validSince = validSince
  }

  if (message.payload.validUntil) {
    const validUntil = fuzzyDates.convert(message.payload.validUntil)[1]
    message.payload.validSince = validUntil
  }

  const actionDesc = {
    [operation]: {
      _index: message.meta.dataset,
      _type: message.payload.type,
      _id: message.payload.id
    }
  }

  let bulkOperations = [actionDesc]

  // When removing no document is needed
  if (message.action !== 'delete') {
    bulkOperations.push(message.payload)
  }

  return bulkOperations
}
