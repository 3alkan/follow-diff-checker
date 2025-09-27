# Instagram export JSON formats

This project parses the JSON files as exported by Instagram. Here are the parts we use.

## followers_1.json
Array of objects; each has `string_list_data` with one element containing the username, profile URL, and timestamp when the user followed you.

Example (simplified):

```json
[
  {
    "string_list_data": [
      {
        "href": "https://www.instagram.com/username/",
        "value": "username",
        "timestamp": 1700000000
      }
    ]
  }
]
```

## following.json
Object with `relationships_following` array; same inner shape with `string_list_data`.

Example (simplified):

```json
{
  "relationships_following": [
    {
      "string_list_data": [
        {
          "href": "https://www.instagram.com/anotheruser/",
          "value": "anotheruser",
          "timestamp": 1700001000
        }
      ]
    }
  ]
}
```

## Notes
- The project keys by the `value` field (username) to compute set differences.
- Timestamps are formatted to Türkiye local time (Europe/Istanbul) in output reports.
- If a record is malformed or missing fields, it is skipped.
