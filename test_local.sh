curl localhost:8282 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
      "message": {
        "data":"eyJldmVudF9pZCI6ICI5MzE5NDA0Yy1hMDJhLTQwMzgtYWQ4OC1kNzUwMmU3OGMyMDMiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMjA6MTM6MzMuNjk1MDE0KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjliZWRjMDNlLTg0MTUtNDZkYi1hYTcwLTc4MjQ5MGNkZmYzMSIsICJyZWxhdGVkX3RvIjogIjZjYjgyMTgyLTUxYjItNDMwOS1iYTgzLWM5OWVkOGU2MWFlOCIsICJvd25lciI6ICJuYSIsICJkYXRhX3Byb3ZpZGVyX2lkIjogImQ4OGFjNTIwLTJiZjYtNGU2Yi1hYjA5LTM4ZWQxZWM2OTQ3YSIsICJzb3VyY2VfaWQiOiAiZWEyZDVmY2EtNzUyYS00YTQ0LWIxNzAtNjY4ZDc4MGRiODVlIiwgImV4dGVybmFsX3NvdXJjZV9pZCI6ICJndW5kaXRlc3QiLCAiZmlsZV9wYXRoIjogImF0dGFjaG1lbnRzLzliZWRjMDNlLTg0MTUtNDZkYi1hYTcwLTc4MjQ5MGNkZmYzMV93aWxkX2RvZy1tYWxlLnN2ZyIsICJvYnNlcnZhdGlvbl90eXBlIjogImF0dCJ9LCAiZXZlbnRfdHlwZSI6ICJBdHRhY2htZW50UmVjZWl2ZWQifQ==",
        "attributes":{
          "gundi_version":"v2",
          "provider_key":"gundi_traptagger_d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
          "gundi_id":"9bedc03e-8415-46db-aa70-782490cdff31",
          "related_to": "6cb82182-51b2-4309-ba83-c99ed8e61ae8",
          "stream_type":"att",
          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
          "external_source_id":"gunditest",
          "destination_id":"79bef222-74aa-4065-88a8-ac9656246693",
          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
          "annotations":"{}",
          "tracing_context":"{}"
        },
        "messageId": "9155786613739819",
        "message_id": "9155786613739819",
        "publishTime": "2024-08-12T12:40:00.789Z",
        "publish_time": "2024-07-18T12:40:00.789Z"
      },
      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
    }'

# AttachmentReceived
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICI5MzE5NDA0Yy1hMDJhLTQwMzgtYWQ4OC1kNzUwMmU3OGMyMDMiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMjA6MTM6MzMuNjk1MDE0KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjliZWRjMDNlLTg0MTUtNDZkYi1hYTcwLTc4MjQ5MGNkZmYzMSIsICJyZWxhdGVkX3RvIjogIjZjYjgyMTgyLTUxYjItNDMwOS1iYTgzLWM5OWVkOGU2MWFlOCIsICJvd25lciI6ICJuYSIsICJkYXRhX3Byb3ZpZGVyX2lkIjogImY4NzBlMjI4LTRhNjUtNDBmMC04ODhjLTQxYmRjMTEyNGMzYyIsICJzb3VyY2VfaWQiOiAiTm9uZSIsICJleHRlcm5hbF9zb3VyY2VfaWQiOiAiTm9uZSIsICJmaWxlX3BhdGgiOiAiYXR0YWNobWVudHMvOWJlZGMwM2UtODQxNS00NmRiLWFhNzAtNzgyNDkwY2RmZjMxX3dpbGRfZG9nLW1hbGUuc3ZnIiwgIm9ic2VydmF0aW9uX3R5cGUiOiAiYXR0In0sICJldmVudF90eXBlIjogIkF0dGFjaG1lbnRSZWNlaXZlZCJ9",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "gundi_id":"9bedc03e-8415-46db-aa70-782490cdff31",
#          "related_to": "6cb82182-51b2-4309-ba83-c99ed8e61ae8",
#          "stream_type":"att",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        },
#        "messageId": "9155786613739819",
#        "message_id": "9155786613739819",
#        "publishTime": "2024-08-12T12:40:00.789Z",
#        "publish_time": "2024-07-18T12:40:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventReceived for ER
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
#        },
#        "messageId": "9155786613739819",
#        "message_id": "9155786613739819",
#        "publishTime": "2024-08-12T12:40:00.789Z",
#        "publish_time": "2024-07-18T12:40:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventUpdateReceived for WPS Watch
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
#          "destination_id":"79bef222-74aa-4065-88a8-ac9656246693",
#          "data_provider_id":"ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        }
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventReceived for SMART
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICIwYTM1OGQ5YS0wZTMwLTQzZDItYTY1Ni04MjFhZDc2MDZmNWEiLCAidGltZXN0YW1wIjogIjIwMjQtMDgtMDIgMTA6NDY6NDEuODAwODI1KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjU0NmI5MjdiLTU3OGMtNDUwNC05OWNlLWE0Mjg5NTI4NDk0MSIsICJvd25lciI6ICJhOTFiNDAwYi00ODJhLTQ1NDYtOGZjYi1lZTQyYjAxZGVlYjYiLCAiZGF0YV9wcm92aWRlcl9pZCI6ICJkODhhYzUyMC0yYmY2LTRlNmItYWIwOS0zOGVkMWVjNjk0N2EiLCAiYW5ub3RhdGlvbnMiOiB7fSwgInNvdXJjZV9pZCI6ICIyOTY2OWIxNy1jODg4LTRjNmQtODdiNS1kOGI5ZTE0ZTM0MmQiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImRlZmF1bHQtc291cmNlIiwgInJlY29yZGVkX2F0IjogIjIwMjQtMDgtMDIgMTA6NDY6MTArMDA6MDAiLCAibG9jYXRpb24iOiB7ImxhdCI6IDEzLjY4ODYzNCwgImxvbiI6IDEzLjc4MzA2NywgImFsdCI6IDAuMH0sICJ0aXRsZSI6ICJBbmltYWxzIDAyIChUZXN0IE1hcmlhbm8pIiwgImV2ZW50X3R5cGUiOiAiYW5pbWFscyIsICJldmVudF9kZXRhaWxzIjogeyJ0YXJnZXRzcGVjaWVzIjogInJlcHRpbGVzLnB5dGhvbnNwcCIsICJ3aWxkbGlmZW9ic2VydmF0aW9udHlwZSI6ICJkaXJlY3RvYnNlcnZhdGlvbiIsICJhZ2VvZnNpZ25hbmltYWwiOiAiZnJlc2giLCAibnVtYmVyb2ZhbmltYWwiOiAxfSwgIm9ic2VydmF0aW9uX3R5cGUiOiAiZXYifSwgImV2ZW50X3R5cGUiOiAiRXZlbnRSZWNlaXZlZCJ9",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"546b927b-578c-4504-99ce-a42895284941",
#          "related_to": "None",
#          "stream_type":"ev",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"b42c9205-5228-49e0-a75b-ebe5b6a9f78e",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        },
#        "messageId": "9155786613739819",
#        "message_id": "9155786613739819",
#        "publishTime": "2024-08-12T12:40:00.789Z",
#        "publish_time": "2024-07-18T12:40:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventUpdateReceived for ER
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
#        },
#        "messageId": "9155786613739819",
#        "message_id": "9155786613739819",
#        "publishTime": "2024-08-12T12:40:00.789Z",
#        "publish_time": "2024-07-18T12:40:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventUpdateReceived for SMART
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICJjMTQxMmZiZi0xNTcwLTQ0YjAtOGE3ZC00MTFlZGUzMDFmMzciLCAidGltZXN0YW1wIjogIjIwMjQtMDgtMDIgMTE6MTg6MDQuNzIyNTI4KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjU0NmI5MjdiLTU3OGMtNDUwNC05OWNlLWE0Mjg5NTI4NDk0MSIsICJyZWxhdGVkX3RvIjogIk5vbmUiLCAib3duZXIiOiAiYTkxYjQwMGItNDgyYS00NTQ2LThmY2ItZWU0MmIwMWRlZWI2IiwgImRhdGFfcHJvdmlkZXJfaWQiOiAiZDg4YWM1MjAtMmJmNi00ZTZiLWFiMDktMzhlZDFlYzY5NDdhIiwgInNvdXJjZV9pZCI6ICIyOTY2OWIxNy1jODg4LTRjNmQtODdiNS1kOGI5ZTE0ZTM0MmQiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImRlZmF1bHQtc291cmNlIiwgImNoYW5nZXMiOiB7InRpdGxlIjogIkFuaW1hbHMgMDIgRWRpdGVkIChUZXN0IE1hcmlhbm8pIiwgInJlY29yZGVkX2F0IjogIjIwMjQtMDgtMDIgMTA6NDY6MTArMDA6MDAiLCAibG9jYXRpb24iOiB7ImxhdCI6IDEzLjEyMzQ1NiwgImxvbiI6IDEzLjEyMzQ1Nn0sICJldmVudF90eXBlIjogImFuaW1hbHMiLCAiZXZlbnRfZGV0YWlscyI6IHsidGFyZ2V0c3BlY2llcyI6ICJyZXB0aWxlcy5weXRob25zcHAiLCAid2lsZGxpZmVvYnNlcnZhdGlvbnR5cGUiOiAiZGlyZWN0b2JzZXJ2YXRpb24iLCAiYWdlb2ZzaWduYW5pbWFsIjogImZyZXNoIiwgIm51bWJlcm9mYW5pbWFsIjogMn19LCAib2JzZXJ2YXRpb25fdHlwZSI6ICJldnUifSwgImV2ZW50X3R5cGUiOiAiRXZlbnRVcGRhdGVSZWNlaXZlZCJ9",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"546b927b-578c-4504-99ce-a42895284941",
#          "related_to": "None",
#          "stream_type":"evu",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"b42c9205-5228-49e0-a75b-ebe5b6a9f78e",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        },
#        "messageId": "9155786613739819",
#        "message_id": "9155786613739819",
#        "publishTime": "2024-08-12T12:40:00.789Z",
#        "publish_time": "2024-07-18T12:40:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventUpdateReceived for SMART - lat only (invalid)
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICJjMTQxMmZiZi0xNTcwLTQ0YjAtOGE3ZC00MTFlZGUzMDFmMzciLCAidGltZXN0YW1wIjogIjIwMjQtMDgtMDIgMTE6MTg6MDQuNzIyNTI4KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjU0NmI5MjdiLTU3OGMtNDUwNC05OWNlLWE0Mjg5NTI4NDk0MSIsICJyZWxhdGVkX3RvIjogIk5vbmUiLCAib3duZXIiOiAiYTkxYjQwMGItNDgyYS00NTQ2LThmY2ItZWU0MmIwMWRlZWI2IiwgImRhdGFfcHJvdmlkZXJfaWQiOiAiZDg4YWM1MjAtMmJmNi00ZTZiLWFiMDktMzhlZDFlYzY5NDdhIiwgInNvdXJjZV9pZCI6ICIyOTY2OWIxNy1jODg4LTRjNmQtODdiNS1kOGI5ZTE0ZTM0MmQiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImRlZmF1bHQtc291cmNlIiwgImNoYW5nZXMiOiB7InRpdGxlIjogIkFuaW1hbHMgMDIgRWRpdGVkIChUZXN0IE1hcmlhbm8pIiwgInJlY29yZGVkX2F0IjogIjIwMjQtMDgtMDIgMTA6NDY6MTArMDA6MDAiLCAibG9jYXRpb24iOiB7ImxhdCI6IDEzLjEyMzQ1Nn0sICJldmVudF90eXBlIjogImFuaW1hbHMiLCAiZXZlbnRfZGV0YWlscyI6IHsidGFyZ2V0c3BlY2llcyI6ICJyZXB0aWxlcy5weXRob25zcHAiLCAid2lsZGxpZmVvYnNlcnZhdGlvbnR5cGUiOiAiZGlyZWN0b2JzZXJ2YXRpb24iLCAiYWdlb2ZzaWduYW5pbWFsIjogImZyZXNoIiwgIm51bWJlcm9mYW5pbWFsIjogMn19LCAib2JzZXJ2YXRpb25fdHlwZSI6ICJldnUifSwgImV2ZW50X3R5cGUiOiAiRXZlbnRVcGRhdGVSZWNlaXZlZCJ9",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"546b927b-578c-4504-99ce-a42895284941",
#          "related_to": "None",
#          "stream_type":"evu",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"b42c9205-5228-49e0-a75b-ebe5b6a9f78e",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        },
#        "messageId": "9155786613739819",
#        "message_id": "9155786613739819",
#        "publishTime": "2024-08-12T12:40:00.789Z",
#        "publish_time": "2024-07-18T12:40:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventUpdateReceived for SMART - not location (valid)
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICJjMTQxMmZiZi0xNTcwLTQ0YjAtOGE3ZC00MTFlZGUzMDFmMzciLCAidGltZXN0YW1wIjogIjIwMjQtMDgtMDIgMTE6MTg6MDQuNzIyNTI4KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjU0NmI5MjdiLTU3OGMtNDUwNC05OWNlLWE0Mjg5NTI4NDk0MSIsICJyZWxhdGVkX3RvIjogIk5vbmUiLCAib3duZXIiOiAiYTkxYjQwMGItNDgyYS00NTQ2LThmY2ItZWU0MmIwMWRlZWI2IiwgImRhdGFfcHJvdmlkZXJfaWQiOiAiZDg4YWM1MjAtMmJmNi00ZTZiLWFiMDktMzhlZDFlYzY5NDdhIiwgInNvdXJjZV9pZCI6ICIyOTY2OWIxNy1jODg4LTRjNmQtODdiNS1kOGI5ZTE0ZTM0MmQiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImRlZmF1bHQtc291cmNlIiwgImNoYW5nZXMiOiB7InRpdGxlIjogIkFuaW1hbHMgMTAgRWRpdGVkIE9ic3YgKFRlc3QgTWFyaWFubykiLCAicmVjb3JkZWRfYXQiOiAiMjAyNC0wOC0wMiAxMDo0NjoxMCswMDowMCIsICJldmVudF90eXBlIjogImFuaW1hbHMiLCAiZXZlbnRfZGV0YWlscyI6IHsidGFyZ2V0c3BlY2llcyI6ICJyZXB0aWxlcy5weXRob25zcHAiLCAid2lsZGxpZmVvYnNlcnZhdGlvbnR5cGUiOiAiZGlyZWN0b2JzZXJ2YXRpb24iLCAiYWdlb2ZzaWduYW5pbWFsIjogImZyZXNoIiwgIm51bWJlcm9mYW5pbWFsIjogNX19LCAib2JzZXJ2YXRpb25fdHlwZSI6ICJldnUifSwgImV2ZW50X3R5cGUiOiAiRXZlbnRVcGRhdGVSZWNlaXZlZCJ9",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"546b927b-578c-4504-99ce-a42895284941",
#          "related_to": "None",
#          "stream_type":"evu",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"b42c9205-5228-49e0-a75b-ebe5b6a9f78e",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        },
#        "messageId": "9155786613739819",
#        "message_id": "9155786613739819",
#        "publishTime": "2024-08-12T12:40:00.789Z",
#        "publish_time": "2024-07-18T12:40:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventUpdateReceived for SMART - update title only (valid)
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICJjMTQxMmZiZi0xNTcwLTQ0YjAtOGE3ZC00MTFlZGUzMDFmMzciLCAidGltZXN0YW1wIjogIjIwMjQtMDgtMDIgMTE6MTg6MDQuNzIyNTI4KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjU0NmI5MjdiLTU3OGMtNDUwNC05OWNlLWE0Mjg5NTI4NDk0MSIsICJyZWxhdGVkX3RvIjogIk5vbmUiLCAib3duZXIiOiAiYTkxYjQwMGItNDgyYS00NTQ2LThmY2ItZWU0MmIwMWRlZWI2IiwgImRhdGFfcHJvdmlkZXJfaWQiOiAiZDg4YWM1MjAtMmJmNi00ZTZiLWFiMDktMzhlZDFlYzY5NDdhIiwgInNvdXJjZV9pZCI6ICIyOTY2OWIxNy1jODg4LTRjNmQtODdiNS1kOGI5ZTE0ZTM0MmQiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImRlZmF1bHQtc291cmNlIiwgImNoYW5nZXMiOiB7InRpdGxlIjogIkFuaW1hbHMgMTAgRWRpdGVkIHRpdGxlIChUZXN0IE1hcmlhbm8pIn0sICJvYnNlcnZhdGlvbl90eXBlIjogImV2dSJ9LCAiZXZlbnRfdHlwZSI6ICJFdmVudFVwZGF0ZVJlY2VpdmVkIn0=",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"546b927b-578c-4504-99ce-a42895284941",
#          "related_to": "None",
#          "stream_type":"evu",
#          "source_id":"ea2d5fca-752a-4a44-b170-668d780db85e",
#          "external_source_id":"Xyz123",
#          "destination_id":"b42c9205-5228-49e0-a75b-ebe5b6a9f78e",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        },
#        "messageId": "9155786613739819",
#        "message_id": "9155786613739819",
#        "publishTime": "2024-08-12T12:40:00.789Z",
#        "publish_time": "2024-07-18T12:40:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
