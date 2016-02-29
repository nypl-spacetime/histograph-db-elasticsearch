var config = require('histograph-config')
var H = require('highland')
var elasticsearch = require('elasticsearch')
var esClient = new elasticsearch.Client({
  host: config.elasticsearch.host + ':' + config.elasticsearch.port
})
var defaultMapping = require('./default-mapping')
var normalize = require('histograph-uri-normalizer').normalize
var turf = {
  extent: require('turf-extent')
}

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

// Create named index
var createIndex = H.wrapCallback(esClient.indices.create.bind(esClient.indices))

module.exports.bulk = function (messages, callback) {
  var pitMessages = messages
    .filter((i) => i.type === 'pit')

  if (pitMessages.length) {
    H(pitMessages)
      .map((i) => i.dataset)
      .uniq()
      .map(function (dataset) {
        // Create ES index for each dataset
        // turn name into options for `esClient.indices.create`
        return {
          index: dataset,
          body: defaultMapping
        }
      })
      .map(createIndex)
      .series()
      .errors(function (err) {
        if (err && err.message.includes('index_already_exists_exception')) {
          console.log('Index already exists — this is fine! 🚜')
        } else {
          console.log('Failed creating index')
          console.error(err, err && err.message)
        }
      })
      .collect()
      .each(function () {
        // Tell it
        console.log('Created all indices!')

        H(pitMessages)
          .map(toElastic)
          .flatten()
          .toArray((bulkOperations) => {
            if (bulkOperations.length) {
              esClient.bulk({body: bulkOperations}, function (err, resp) {
                var r = resp || {took: 0, errors: false, items: []}
                var length = (r.items && r.items.length) || 0
                console.log('Elasticsearch => %d indexed, took %dms, errors: %s', length, r.took, r.errors)
                callback(err)
              })
            } else {
              callback()
            }
          })
      })
  } else {
    callback()
  }
}

// Index into elasticsearch
var OP_MAP = {
  add: 'index',
  update: 'index',
  delete: 'delete'
}

// Convert message to Elasticsearch bulk operation
function toElastic (message) {
  // select appropriate ES operation
  var operation = OP_MAP[message.action]

  // { "action": { _index ... } , see
  // https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference-2-0.html

  // normalize id
  var id = normalize(message.data.id || message.data.uri, message.dataset)
  message.data.id = id
  delete message.data.uri

  if (message.data.geometry) {
    // turf.extent returns bounding box array, in west, south, east, north order
    var extent = turf.extent(message.data.geometry)

    // The Elasticsearch geo_point type expects [lon, lat] arrays
    message.data.northWest = [extent[0], extent[3]]
    message.data.southEast = [extent[2], extent[1]]
  }

  var actionDesc = {}
  actionDesc[operation] = {
    _index: message.dataset,
    _type: message.data.type,
    _id: message.data.id
  }

  var bulkOperations = [actionDesc]

  // when removing no document is needed
  if (message.action !== 'delete') {
    bulkOperations.push(message.data)
  }

  return bulkOperations
}
