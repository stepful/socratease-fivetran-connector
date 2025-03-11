# Socratease Fivetran Connector

## Docs

- fivetran connector setup docs: https://fivetran.com/docs/connector-sdk/technical-reference#technicaldetailsoperations
- example of fivetran sdk in action: https://github.com/fivetran/fivetran_connector_sdk/blob/main/examples/common_patterns_for_connectors/pagination/offset_based/connector.py
- socratease (quiz provider) API docs: https://help.socratease.co/infra/rest-apis

## Socratease API Examples

```rb
# Get all quiz definitions.
# This does NOT return the question definitions for each quiz.
# Example API response:
# {"data": {"quiz": [{"type" => "quiz",
# "created_at" => "2024-04-06T23:07:23.046753+00:00",
# "num_screens" => 30,
# "num_questions" => 30,
# "access_level" => "public",
# "title" =>
#  "TS-C Daily Assignment: Surgical Procedure Set-Up: Preference Card & Operating Room Preparation  Review",
# "total_points" => 38,
# "max_attempts" => 1000,
# "meta_data" =>
#  {"conclusion" => {"text" => ""},
#   "cs_details" => {},
#   "instructions" => "",
#   "introduction" => {"text" => ""},
#   "thumbnail_url" => "",
#   "topics_covered" => [],
#   "file_upload_url" => ""},
# "settings" =>
#  {"auto_submit" => true,
#   "parse_latex" => false,
#   "timer_settings" => {"duration" => 1800},
#   "detect_tab_switch" => true,
#   "is_gsheet_enabled" => false,
#   "disable_copy_paste" => true,
#   "enable_certificate" => false,
#   "randomise_questions" => true,
#   "grade_release_method" => "immediate",
#   "show_screen_by_screen" => false,
#   "tab_switch_auto_submit_limit" => "2"},
# "visible" => true,
# "unit_id" => "pio-sbb-qqg"}]}}
def self.quiz_definitions
  api_request("/v1/units/", params: { type: "quiz" })
    .fetch_dig("data", "quiz")
end

# Get all the attempts for a quiz
# Example API response (if you do NOT provide `soc_user_id`):
# {
#   "is_registered": true,
#   "username": "mymail@gmail.com",
#   "first_name": "my name",
#   "last_name": "",
#   "lookup_key": null,
#   "lookup_key_2": null,
#   "attempt_num": 1,
# NOTE: bug: `percentage_decimal` is sometimes missing. we fallback to `points_aggregate / unit_total_points`
#   "percentage_decimal": 1.0,
#   "percentage_decimal_rounded_to_4": 1.0,
#   "pr_right_1st_attempt": null,
#   "points_aggregate": 10.0,
# NOTE: bug: `unit_total_points` is sometimes off by -1. we do not handle this bug.
#   "unit_total_points": 10,
#   "user_id": "cOFOgmyX",
#   "up_id": 1_434_648,
#   "up_meta_data": { ... },
#   "certificate_url": null,
#   "trust_score": null,
#   "num_questions_attempted": 10,
#   "grade_finalized": true,
#   "finished": true,
#   "grade_released": true,
#   "finished_at": "2024-02-21T18:55:54.662849+00:00",
#   "time_taken_in_seconds": 133.132954,
#   "started_at": "2024-02-21T18:53:41.529895+00:00",
# }
def self.quiz_attempts(soc_quiz_id:)
  api_request(
    "/v1/user-progress/",
    params: {
      unit_id: soc_quiz_id,
    }.compact,
  )
end


# Get a single quiz-attempt and the question-attempts for that quiz-attempt.
# Example response:
# "data": { "responses": { "194101": [{
#   "user_response_id": 73189645,
#   "response": { "chosen_ind": 3 },
#   "meta_data": {},
#   "points_scored": 1.0,
#   "got_correct_answer": true,
#   "cs_cs_assn_id": null,
#   "cs_unit_assn_id": 194101,
#   "points_assigned": 1,
#   "screen_index": 1,
#   "num_points": 1,
#   "type": "mcq_1",
#   "contents": {
#     "tags": [],
#     "choices": [
#       { "index": 0, "choice": "Performing surgical procedures" },
#       { "index": 1, "choice": "Administering medications to patients" },
#       { "index": 2, "choice": "Diagnosing and treating diseases" },
#       {
#         "index": 3,
#         "choice": "Compounding medications according to prescriptions"
#       }
#     ],
#     "feedback": "",
#     "markdown": false,
#     "question": "Which of the following tasks is typically performed by a pharmacy technician?",
#     "question_title": "",
#     "correct_ans_ind": 3,
#     "randomize_options": true,
#     "non_skippable_content": false
#   },
#   "cs_int_id": 207431,
#   "tags": []
# }],
def self.quiz_question_attempts(soc_user_id:, soc_quiz_id:, attempt_num:)
  api_request(
    "/v1/user-responses/",
    params: {
      user_id: soc_user_id,
      unit_id: soc_quiz_id,
      attempt_num: attempt_num,
    },
  )
  .fetch_dig("data", "responses")
  .filter_map do |_unit_assn_id, responses|
    responses.last # AFAIK there's exactly element in this array
  end
end
```