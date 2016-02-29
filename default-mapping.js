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
          'index': 'not_analyzed',
          'type': 'string'
        },
        'id': {
          'index': 'not_analyzed',
          'store': true,
          'type': 'string'
        },
        'type': {
          'index': 'not_analyzed',
          'type': 'string'
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
          'index': 'not_analyzed',
          'type': 'string'
        },
        'validSince': {
          'format': 'date_optional_time',
          'type': 'date'
        },
        'validUntil': {
          'format': 'date_optional_time',
          'type': 'date'
        }
      }
    }
  }
}
