'use strict'

var config = require('histograph-config')
var H = require('highland')
var R = require('ramda')
var elasticsearch = require('elasticsearch')
var esClient = new elasticsearch.Client({
  host: config.elasticsearch.host + ':' + config.elasticsearch.port
})
var turf = {
  extent: require('turf-extent')
}

var defaultMapping = require('./default-mapping')

var pageSize = 100

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

module.exports.search = function (params, callback) {
  var onlyIds = false

  if (!params) {
    params = {}
  }

  var index = '*'
  if (params.dataset) {
    index = params.dataset.join(',')
  }

  var query = baseQuery()

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
  }).then(function (resp) {
    // TODO: convert IDs + URIs
    callback(null, resp.hits.hits.map((hit) => {
      return Object.assign({dataset: hit._index}, hit._source)
    }))
  },

  function (err) {
    callback(err)
  })
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
  var mapping = Object.assign({}, defaultMapping)

  var context = message.payload['@context']
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

    var properties = R.fromPairs(pairs)

    mapping.mappings['_default_'].properties.data = {
      type: 'nested',
      include_in_parent: true,
      properties: properties
    }
  }

  return mapping
}

// Curried ES functions
var createIndex = R.curry(esClient.indices.create.bind(esClient.indices))
var deleteIndex = R.curry(esClient.indices.delete.bind(esClient.indices))

module.exports.bulk = function (messages, callback) {
  var functions = []
  var pitMessages = []

  var elasticBulk = (pitMessages) => {
    if (pitMessages.length) {
      return (callback) => {
        esClient.bulk({body: R.flatten(pitMessages.map(toElastic))}, function (err, resp) {
          var r = resp || {took: 0, errors: false, items: []}
          var length = (r.items && r.items.length) || 0
          console.log('Elasticsearch => %d indexed, took %dms, errors: %s', length, r.took, r.errors)
          callback(err)
        })
      }
    }
    return []
  }

  messages
    .filter((message) => message.type === 'pit' || message.type === 'dataset')
    .forEach((message) => {
      if (message.type === 'pit') {
        pitMessages.push(message)
      } else if (message.type === 'dataset') {
        functions = R.concat(functions, elasticBulk(pitMessages))

        // Reset pitMessages
        pitMessages = []

        if (message.action === 'create') {
          let f = createIndex({
            index: message.payload.id,
            body: getMapping(message)
          }, R.__)

          functions.push(f)
        } else if (message.action === 'delete') {
          let f = deleteIndex({
            index: message.payload.id
          }, R.__)

          functions.push(f)
        }
      }
    })

  functions = R.concat(functions, elasticBulk(pitMessages))

  H(functions)
    .nfcall([])
    .series()
    .done(() => {
      callback()
    })
}

// Index into elasticsearch
const OP_MAP = {
  create: 'index',
  update: 'index',
  delete: 'delete'
}

// Convert message to Elasticsearch bulk operation
function toElastic (message) {
  // select appropriate ES operation
  var operation = OP_MAP[message.action]

  // { "action": { _index ... } , see
  // https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference-2-0.html
  //
  if (message.payload.geometry) {
    // turf.extent returns bounding box array, in west, south, east, north order
    var extent = turf.extent(message.payload.geometry)

    // The Elasticsearch geo_point type expects [lon, lat] arrays
    message.payload.northWest = [extent[0], extent[3]]
    message.payload.southEast = [extent[2], extent[1]]
  }

  var actionDesc = {}
  actionDesc[operation] = {
    _index: message.meta.dataset,
    _type: message.payload.type,
    _id: message.payload.id
  }

  var bulkOperations = [actionDesc]

  // When removing no document is needed
  if (message.action !== 'delete') {
    bulkOperations.push(message.payload)
  }

  return bulkOperations
}
