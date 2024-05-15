resource "google_monitoring_dashboard" "dashboard" {
  project        = var.project_id
  dashboard_json = <<EOF
{
  "displayName": "Gundi Transformer - ${var.env}",
  "dashboardFilters": [],
  "gridLayout": {
    "columns": "2",
    "widgets": [
      {
        "title": "Pub/Sub Unacked messages",
        "xyChart": {
          "chartOptions": {
            "mode": "COLOR"
          },
          "dataSets": [
            {
              "breakdowns": [],
              "dimensions": [],
              "legendTemplate": "messages",
              "measures": [],
              "minAlignmentPeriod": "60s",
              "plotType": "LINE",
              "targetAxis": "Y1",
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "crossSeriesReducer": "REDUCE_SUM",
                    "groupByFields": [],
                    "perSeriesAligner": "ALIGN_MEAN"
                  },
                  "filter": "metric.type=\"pubsub.googleapis.com/topic/num_unacked_messages_by_region\" resource.type=\"pubsub_topic\" resource.label.\"topic_id\"=\"raw-observations-${var.env}\""
                }
              }
            }
          ],
          "thresholds": [
            {
              "label": "",
              "targetAxis": "Y1",
              "value": 100
            }
          ],
          "timeshiftDuration": "0s",
          "yAxis": {
            "label": "Unacked messages",
            "scale": "LINEAR"
          }
        }
      },
      {
        "title": "Pub/Sub Message count",
        "xyChart": {
          "chartOptions": {
            "mode": "COLOR"
          },
          "dataSets": [
            {
              "breakdowns": [],
              "dimensions": [],
              "measures": [],
              "minAlignmentPeriod": "60s",
              "plotType": "LINE",
              "targetAxis": "Y1",
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "crossSeriesReducer": "REDUCE_SUM",
                    "groupByFields": [],
                    "perSeriesAligner": "ALIGN_RATE"
                  },
                  "filter": "metric.type=\"pubsub.googleapis.com/topic/send_request_count\" resource.type=\"pubsub_topic\" resource.label.\"topic_id\"=\"raw-observations-${var.env}\""
                }
              }
            }
          ],
          "thresholds": [],
          "yAxis": {
            "label": "",
            "scale": "LINEAR"
          }
        }
      },
      {
        "title": "Cloud Run - Failed request count",
        "xyChart": {
          "chartOptions": {
            "mode": "COLOR"
          },
          "dataSets": [
            {
              "breakdowns": [],
              "dimensions": [],
              "measures": [],
              "minAlignmentPeriod": "60s",
              "plotType": "LINE",
              "targetAxis": "Y1",
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "crossSeriesReducer": "REDUCE_SUM",
                    "groupByFields": [
                      "metric.label.\"response_code_class\""
                    ],
                    "perSeriesAligner": "ALIGN_RATE"
                  },
                  "filter": "metric.type=\"run.googleapis.com/request_count\" resource.type=\"cloud_run_revision\" resource.label.\"service_name\"=\"routing-transformer-service-${var.env}\" metric.label.\"response_code_class\"!=\"2xx\""
                }
              }
            }
          ],
          "thresholds": [
            {
              "label": "",
              "targetAxis": "Y1",
              "value": 5
            }
          ],
          "yAxis": {
            "label": "Request Count",
            "scale": "LINEAR"
          }
        }
      },
      {
        "title": "Cloud Run - Request Latency",
        "xyChart": {
          "chartOptions": {
            "mode": "COLOR"
          },
          "dataSets": [
            {
              "breakdowns": [],
              "dimensions": [],
              "legendTemplate": "95th percentile",
              "measures": [],
              "minAlignmentPeriod": "60s",
              "plotType": "LINE",
              "targetAxis": "Y1",
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "crossSeriesReducer": "REDUCE_PERCENTILE_95",
                    "groupByFields": [],
                    "perSeriesAligner": "ALIGN_DELTA"
                  },
                  "filter": "metric.type=\"run.googleapis.com/request_latencies\" resource.type=\"cloud_run_revision\" resource.label.\"service_name\"=\"routing-transformer-service-${var.env}\""
                }
              }
            }
          ],
          "thresholds": [
            {
              "label": "",
              "targetAxis": "Y1",
              "value": 15000
            }
          ],
          "yAxis": {
            "label": "",
            "scale": "LINEAR"
          }
        }
      }
    ]
  }
}

EOF
}

data "google_secret_manager_secret_version" "slack_bot_user_oauth_token" {
  secret  = "slack-notifications-bot-token-${var.env}"
  project = var.project_id
}

resource "google_monitoring_notification_channel" "slack-channel" {
  display_name = "Gundi Routing Slack - ${var.env}"
  project      = var.project_id
  type         = "slack"
  labels = {
    "channel_name" = "#er-gundi-monitoring"
    "team"         = "Nimble Gravity"
  }
  sensitive_labels {
    auth_token = data.google_secret_manager_secret_version.slack_bot_user_oauth_token.secret_data
  }
}

resource "google_monitoring_alert_policy" "unacked_messages" {
  display_name = "Transformer ${var.env} - Unacked messages"
  project      = var.project_id
  combiner     = "OR"
  conditions {
    display_name = "Transformer ${var.env} - Unacked messages"
    condition_threshold {
      filter          = "resource.type = \"pubsub_topic\" AND resource.labels.topic_id = \"raw-observations-${var.env}\" AND metric.type = \"pubsub.googleapis.com/topic/num_unacked_messages_by_region\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 100
      aggregations {
        alignment_period   = "900s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.slack-channel.name]
}

resource "google_monitoring_alert_policy" "run_failed_requests" {
  display_name = "Transformer ${var.env} - Cloud Run failed requests"
  project      = var.project_id
  combiner     = "OR"
  conditions {
    display_name = "Transformer ${var.env} - Cloud Run failed requests"
    condition_threshold {
      filter          = "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = \"routing-transformer-service-${var.env}\" AND metric.type = \"run.googleapis.com/request_count\" AND metric.labels.response_code_class != \"2xx\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 5
      aggregations {
        alignment_period     = "900s"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["metric.label.response_code_class"]
        per_series_aligner   = "ALIGN_RATE"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.slack-channel.name]
}

resource "google_monitoring_alert_policy" "run_request_latency" {
  display_name = "Transformer ${var.env} - Cloud Run request latency"
  project      = var.project_id
  combiner     = "OR"
  conditions {
    display_name = "Transformer ${var.env} - Cloud Run request latency"
    condition_threshold {
      filter          = "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = \"routing-transformer-service-${var.env}\" AND metric.type = \"run.googleapis.com/request_latencies\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 15000
      aggregations {
        alignment_period     = "900s"
        cross_series_reducer = "REDUCE_PERCENTILE_95"
        per_series_aligner   = "ALIGN_DELTA"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.slack-channel.name]
}