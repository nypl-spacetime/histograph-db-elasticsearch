module.exports = {
  'settings': {
    'number_of_shards': 5,
    'number_of_replicas': 0,
    'analysis': {
      'analyzer': {
        'lowercase': {
          'type': 'custom',
          'filter': 'lowercase',
          'tokenizer': 'keyword'
        }
      }
    }
  },
  'mappings': {
    '_default_': {
      'dynamic': false,
      'properties': {
        'northWest': {
          'type': 'geo_point'
        },
        'southEast': {
          'type': 'geo_point'
        },
        'uri': {
          'type': 'string',
          'index': 'not_analyzed'
        },
        'id': {
          'type': 'string',
          'index': 'not_analyzed',
          'store': true
        },
        'type': {
          'type': 'string',
          'index': 'not_analyzed'
        },
        'name': {
          'fields': {
            'analyzed': {
              'index': 'analyzed',
              'store': true,
              'type': 'string'
            },
            'exact': {
              'analyzer': 'lowercase',
              'store': true,
              'type': 'string'
            }
          },
          'type': 'string'
        },
        'dataset': {
          'type': 'string',
          'index': 'not_analyzed'
        },
        'validSince': {
          'type': 'date',
          'format': 'date_optional_time'
        },
        'validUntil': {
          'type': 'date',
          'format': 'date_optional_time'
        }
      }
    }
  }
}
