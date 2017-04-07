'use strict'

const R = require('ramda')
const H = require('highland')
const config = require('spacetime-config')
const elasticsearch = require('elasticsearch')
const turf = {
  centroid: require('@turf/centroid'),
  bbox: require('@turf/bbox')
}

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
      filtered: {
        filter: {
          bool: {
            must: []
          }
        },
        query: {
          bool: {
            must: []
          }
        }
      }
    }
  }
}

module.exports.query = function (params, callback) {
  esClient.search(params)
    .then((resp) => callback(null, resp), callback)
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
  if (params.dataset) {
    index = params.dataset.join(',')
  }

  const query = baseQuery()

  if (onlyIds) {
    query._source = [
      '_id'
    ]
  }

  if (params.name) {
    var field = 'name.' + (params.exact ? 'exact' : 'analyzed')

    query.query.filtered.query.bool.must.push({
      query_string: {
        query: params.name,
        fields: [
          field
        ]
      }
    })
  }

  var id = params.uri || params.id
  if (id) {
    query.query.filtered.filter.bool.must.push({
      term: {
        _id: id
      }
    })
  }

  if (params.type) {
    query.query.filtered.filter.bool.must.push({
      or: params.type.map(function (type) {
        return {
          type: {
            value: type
          }
        }
      })
    })
  }

  if (params.intersects) {
    query.query.filtered.filter.bool.must.push({
      or: [
        {
          geo_bounding_box: {
            northWest: {
              top_left: {
                lat: params.intersects[0][1],
                lon: params.intersects[0][0]
              },
              bottom_right: {
                lat: params.intersects[1][1],
                lon: params.intersects[1][0]
              }
            }
          }
        },
        {
          geo_bounding_box: {
            southEast: {
              top_left: {
                lat: params.intersects[0][1],
                lon: params.intersects[0][0]
              },
              bottom_right: {
                lat: params.intersects[1][1],
                lon: params.intersects[1][0]
              }
            }
          }
        }
      ]
    })
  }

  if (params.contains) {
    query.query.filtered.filter.bool.must.push(
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

  if (params.before) {
    query.query.filtered.query.bool.must.push({
      range: {
        validSince: {
          lte: params.before
        }
      }
    })
  }

  if (params.after) {
    query.query.filtered.query.bool.must.push({
      range: {
        validUntil: {
          gte: params.after
        }
      }
    })
  }

  esClient.search({
    index: index,
    body: query
  }).then((response) => {
    // TODO: convert IDs + URIs
    callback(null, response.hits.hits.map((hit) => {
      return Object.assign({dataset: hit._index}, hit._source)
    }))
  }, callback)
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
