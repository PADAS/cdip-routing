curl localhost:8282 \
  -X POST \
  -H "Content-Type: application/json" \
  -H "ce-id: 123451234512345" \
  -H "ce-specversion: 1.0" \
  -H "ce-time: 2024-07-18T17:40:00.789Z" \
  -H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
  -H "ce-source: //pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC" \
  -d '{
      "message": {
        "data":"eyJldmVudF9pZCI6ICIyZGZkZDc3ZS0zY2RlLTQyNjktYjJhOC1kZTAxMzc0ZDMyOTQiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTQ6MTg6NDEuOTUxMjQxKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjJhMWUwZTZjLTMzNGYtNDJmZS05ZDQ1LTEyYzM0ZWQ0ODY2ZiIsICJvd25lciI6ICJhOTFiNDAwYi00ODJhLTQ1NDYtOGZjYi1lZTQyYjAxZGVlYjYiLCAiZGF0YV9wcm92aWRlcl9pZCI6ICJkODhhYzUyMC0yYmY2LTRlNmItYWIwOS0zOGVkMWVjNjk0N2EiLCAiYW5ub3RhdGlvbnMiOiB7fSwgInNvdXJjZV9pZCI6ICJhYzFiOWNkYy1hMTkzLTQ1MTUtYjQ0Ni1iMTc3YmNjNWYzNDIiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImNhbWVyYTEyMyIsICJyZWNvcmRlZF9hdCI6ICIyMDI0LTA3LTI0IDE0OjE4OjEyKzAwOjAwIiwgImxvY2F0aW9uIjogeyJsYXQiOiAxMy42ODg2MzUsICJsb24iOiAxMy43ODMwNjUsICJhbHQiOiAwLjB9LCAidGl0bGUiOiAiQW5pbWFsIERldGVjdGVkIFRlc3QgRXZlbnQiLCAiZXZlbnRfdHlwZSI6ICJ3aWxkbGlmZV9zaWdodGluZ19yZXAiLCAiZXZlbnRfZGV0YWlscyI6IHsic3BlY2llcyI6ICJMaW9uIn0sICJvYnNlcnZhdGlvbl90eXBlIjogImV2In0sICJldmVudF90eXBlIjogIkV2ZW50UmVjZWl2ZWQifQ==",
        "attributes":{
          "gundi_version":"v2",
          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
          "gundi_id":"6cb82182-51b2-4309-ba83-c99ed8e61ae8",
          "related_to": "None",
          "stream_type":"ev",
          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
          "external_source_id":"Xyz123",
          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
          "annotations":"{}",
          "tracing_context":"{}"
        }
      },
      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
    }'
# AttachmentReceived
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICI5MzE5NDA0Yy1hMDJhLTQwMzgtYWQ4OC1kNzUwMmU3OGMyMDMiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMjA6MTM6MzMuNjk1MDE0KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjliZWRjMDNlLTg0MTUtNDZkYi1hYTcwLTc4MjQ5MGNkZmYzMSIsICJyZWxhdGVkX3RvIjogIjZjYjgyMTgyLTUxYjItNDMwOS1iYTgzLWM5OWVkOGU2MWFlOCIsICJvd25lciI6ICJuYSIsICJkYXRhX3Byb3ZpZGVyX2lkIjogImY4NzBlMjI4LTRhNjUtNDBmMC04ODhjLTQxYmRjMTEyNGMzYyIsICJzb3VyY2VfaWQiOiAiTm9uZSIsICJleHRlcm5hbF9zb3VyY2VfaWQiOiAiTm9uZSIsICJmaWxlX3BhdGgiOiAiYXR0YWNobWVudHMvOWJlZGMwM2UtODQxNS00NmRiLWFhNzAtNzgyNDkwY2RmZjMxX3dpbGRfZG9nLW1hbGUuc3ZnIiwgIm9ic2VydmF0aW9uX3R5cGUiOiAiYXR0In0sICJldmVudF90eXBlIjogIkF0dGFjaG1lbnRSZWNlaXZlZCJ9",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"9bedc03e-8415-46db-aa70-782490cdff31",
#          "related_to": "6cb82182-51b2-4309-ba83-c99ed8e61ae8",
#          "stream_type":"att",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        }
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventReceived
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICIyZGZkZDc3ZS0zY2RlLTQyNjktYjJhOC1kZTAxMzc0ZDMyOTQiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTQ6MTg6NDEuOTUxMjQxKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjJhMWUwZTZjLTMzNGYtNDJmZS05ZDQ1LTEyYzM0ZWQ0ODY2ZiIsICJvd25lciI6ICJhOTFiNDAwYi00ODJhLTQ1NDYtOGZjYi1lZTQyYjAxZGVlYjYiLCAiZGF0YV9wcm92aWRlcl9pZCI6ICJkODhhYzUyMC0yYmY2LTRlNmItYWIwOS0zOGVkMWVjNjk0N2EiLCAiYW5ub3RhdGlvbnMiOiB7fSwgInNvdXJjZV9pZCI6ICJhYzFiOWNkYy1hMTkzLTQ1MTUtYjQ0Ni1iMTc3YmNjNWYzNDIiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImNhbWVyYTEyMyIsICJyZWNvcmRlZF9hdCI6ICIyMDI0LTA3LTI0IDE0OjE4OjEyKzAwOjAwIiwgImxvY2F0aW9uIjogeyJsYXQiOiAxMy42ODg2MzUsICJsb24iOiAxMy43ODMwNjUsICJhbHQiOiAwLjB9LCAidGl0bGUiOiAiQW5pbWFsIERldGVjdGVkIFRlc3QgRXZlbnQiLCAiZXZlbnRfdHlwZSI6ICJ3aWxkbGlmZV9zaWdodGluZ19yZXAiLCAiZXZlbnRfZGV0YWlscyI6IHsic3BlY2llcyI6ICJMaW9uIn0sICJvYnNlcnZhdGlvbl90eXBlIjogImV2In0sICJldmVudF90eXBlIjogIkV2ZW50UmVjZWl2ZWQifQ==",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"6cb82182-51b2-4309-ba83-c99ed8e61ae8",
#          "related_to": "None",
#          "stream_type":"ev",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        }
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventUpdateReceived ?
#    -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICJiMTFjN2VlYS03ODMwLTRkMDctODdjOS1iNzdiMDc1ZjEyY2MiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMTkgMTQ6MTk6MDUuNDAzMjc4KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjc4ODY3YTc0LTY3ZjAtNGI1Ni04ZTQ0LTEyNWFlNjY0MDhmZiIsICJyZWxhdGVkX3RvIjogIk5vbmUiLCAib3duZXIiOiAiYTkxYjQwMGItNDgyYS00NTQ2LThmY2ItZWU0MmIwMWRlZWI2IiwgImRhdGFfcHJvdmlkZXJfaWQiOiAiZjg3MGUyMjgtNGE2NS00MGYwLTg4OGMtNDFiZGMxMTI0YzNjIiwgImFubm90YXRpb25zIjogbnVsbCwgInNvdXJjZV9pZCI6ICJhYzFiOWNkYy1hMTkzLTQ1MTUtYjQ0Ni1iMTc3YmNjNWYzNDIiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImNhbWVyYTEyMyIsICJjaGFuZ2VzIjogeyJldmVudF9kZXRhaWxzIjogeyJzcGVjaWVzIjogIndpbGRjYXQifX0sICJvYnNlcnZhdGlvbl90eXBlIjogImV2dSJ9LCAiZXZlbnRfdHlwZSI6ICJFdmVudFVwZGF0ZVJlY2VpdmVkIn0=",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "gundi_id":"189d3730-c6de-4bb7-974e-7bc410e2f4a2",
#          "related_to": "None",
#          "stream_type":"evu",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"a9aa0990-2674-4ff4-8ba2-f9b9a613d7c0",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        }
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'

