import requests
from fivetran_connector_sdk import Logging as log

##############################
# Example API Responses
##############################
# -----------------------------------------
# Get all quiz definitions.
#
# Endpoint: GET /v1/units/?type=quiz
#
# Example API response:
# {
#   "data": {
#     "quiz": [{
#       "type": "quiz",
#       "created_at": "2024-04-06T23:07:23.046753+00:00",
#       "num_screens": 30,
#       "num_questions": 30,
#       "access_level": "public",
#       "title": "TS-C Daily Assignment: Surgical Procedure Set-Up: Preference Card & Operating Room Preparation  Review",
#       "total_points": 38,
#       "max_attempts": 1000,
#       "meta_data": {
#         "conclusion": {"text": ""},
#         "cs_details": {},
#         "instructions": "",
#         "introduction": {"text": ""},
#         "thumbnail_url": "",
#         "topics_covered": [],
#         "file_upload_url": ""
#       },
#       "settings": {
#         "auto_submit": true,
#         "parse_latex": false,
#         "timer_settings": {"duration": 1800},
#         "detect_tab_switch": true,
#         "is_gsheet_enabled": false,
#         "disable_copy_paste": true,
#         "enable_certificate": false,
#         "randomise_questions": true,
#         "grade_release_method": "immediate",
#         "show_screen_by_screen": false,
#         "tab_switch_auto_submit_limit": "2"
#       },
#       "visible": true,
#       "unit_id": "pio-sbb-qqg"
#     }]
#   }
# }
# -----------------------------------------
#
# Get all attempts for a quiz
#
# Endpoint: GET /v1/user-progress/?unit_id=<soc_quiz_id>
#
# Example API response (if you do NOT provide `soc_user_id`):
# {
#   "is_registered": true,
#   "username": "mymail@gmail.com",
#   "first_name": "my name",
#   "last_name": "",
#   "lookup_key": null,
#   "lookup_key_2": null,
#   "attempt_num": 1,
#   "percentage_decimal": 1.0,  // NOTE: bug: sometimes missing, fallback to points_aggregate / unit_total_points
#   "percentage_decimal_rounded_to_4": 1.0,
#   "pr_right_1st_attempt": null,
#   "points_aggregate": 10.0,
#   "unit_total_points": 10,      // NOTE: bug: sometimes off by -1
#   "user_id": "cOFOgmyX",
#   "up_id": 1434648,
#   "up_meta_data": { ... },
#   "certificate_url": null,
#   "trust_score": null,
#   "num_questions_attempted": 10,
#   "grade_finalized": true,
#   "finished": true,
#   "grade_released": true,
#   "finished_at": "2024-02-21T18:55:54.662849+00:00",
#   "time_taken_in_seconds": 133.132954,
#   "started_at": "2024-02-21T18:53:41.529895+00:00"
# }
# -----------------------------------------
#
# Get all question-attempts for a quiz-attempt.
#
# Endpoint: GET /v1/user-responses/?user_id=<soc_user_id>&unit_id=<soc_quiz_id>&attempt_num=<attempt_num>
#
# Example response:
# {
#   "data": {
#     "responses": {
#       "194101": [{
#         "user_response_id": 73189645,
#         "response": { "chosen_ind": 3 },
#         "meta_data": {},
#         "points_scored": 1.0,
#         "got_correct_answer": true,
#         "cs_cs_assn_id": null,
#         "cs_unit_assn_id": 194101,
#         "points_assigned": 1,
#         "screen_index": 1,
#         "num_points": 1,
#         "type": "mcq_1",
#         "contents": {
#           "tags": [],
#           "choices": [
#             { "index": 0, "choice": "Performing surgical procedures" },
#             { "index": 1, "choice": "Administering medications to patients" },
#             { "index": 2, "choice": "Diagnosing and treating diseases" },
#             { "index": 3, "choice": "Compounding medications according to prescriptions" }
#           ],
#           "feedback": "",
#           "markdown": false,
#           "question": "Which of the following tasks is typically performed by a pharmacy technician?",
#           "question_title": "",
#           "correct_ans_ind": 3,
#           "randomize_options": true,
#           "non_skippable_content": false
#         },
#         "cs_int_id": 207431,
#         "tags": []
#       }]
#     }
#   }
# }
# -----------------------------------------


##############################
# Helper Functions
##############################
def get_auth_headers(config):
    """
    Create authorization headers from config.

    Args:
        config (dict): The connector configuration

    Returns:
        dict: Headers with authorization
    """
    api_key = config.get("api_key")
    return {"Authorization": f"Bearer {api_key}"}


def get_base_url(config):
    """
    Get the base URL from config with default fallback.

    Args:
        config (dict): The connector configuration

    Returns:
        str: The base URL for API requests
    """
    return config.get("base_url", "https://api.socratease.com")


##############################
# API Request Functions
##############################
def api_request(*, url, params, headers):
    """
    Make a GET request to the Socratease API.

    Args:
        url (str): The API endpoint URL
        params (dict): Query parameters
        headers (dict): Request headers including authorization

    Returns:
        dict: The JSON response from the API

    Raises:
        RuntimeError: If the API request fails
    """
    resp = requests.get(url=url, headers=headers, params=params)
    if resp.status_code != 200:
        log.severe(f"Request failed for {url} with status {resp.status_code}")
        raise RuntimeError(f"API error {resp.status_code}")
    return resp.json()


def paginated_api_request(
    *, url, params, headers, data_path, limit=100, offset_param="offset"
):
    """
    Make paginated GET requests to the Socratease API.

    This function handles pagination automatically by making multiple requests
    and combining the results until all data is fetched.

    Args:
        url (str): The API endpoint URL
        params (dict): Base query parameters
        headers (dict): Request headers including authorization
        data_path (list): Path to the data array in the response (e.g. ["data", "quiz"])
        limit (int): Maximum number of items per page
        offset_param (str): Name of the offset parameter for pagination

    Returns:
        list: Combined results from all pages

    Raises:
        RuntimeError: If any API request fails
    """
    all_results = []
    offset = 0
    params = params.copy()  # Create a copy to avoid modifying the original

    # Add limit parameter if not already present
    if "limit" not in params:
        params["limit"] = limit

    while True:
        # Update offset for pagination
        params[offset_param] = offset

        # Make the request
        response = api_request(url=url, params=params, headers=headers)

        # Extract data using the provided path
        data = response
        for key in data_path:
            if key in data:
                data = data[key]
            else:
                log.warning(
                    f"Key '{key}' not found in response at path {data_path[:data_path.index(key)]}"
                )
                data = []
                break

        # If data is not a list, wrap it in a list
        if not isinstance(data, list):
            log.warning(f"Expected list at path {data_path}, got {type(data).__name__}")
            if data:  # If there's data but it's not a list
                data = [data]
            else:
                data = []

        # If no data returned, we've reached the end
        if not data:
            break

        # Add results to the combined list
        all_results.extend(data)

        # Log progress
        log.info(f"Fetched {len(data)} items from {url}, offset {offset}")

        # If we got fewer results than the limit, we've reached the end
        if len(data) < params["limit"]:
            break

        # Move to the next page
        offset += params["limit"]

    return all_results


def get_units(*, config, limit=100, offset=0, updated_after=None):
    """
    Get all quiz definitions (units) from the Socratease API.

    Args:
        config (dict): The connector configuration
        limit (int): Maximum number of units to return per page
        offset (int): Starting offset (used internally by paginated_api_request)
        updated_after (str): ISO timestamp to filter units updated after this time

    Returns:
        dict: The API response containing quiz units
    """
    base_url = get_base_url(config)
    headers = get_auth_headers(config)

    params = {
        "type": "quiz",
        "limit": limit,
    }
    if updated_after:
        params["updated_after"] = updated_after

    url = f"{base_url}/v1/units/"

    # For backward compatibility, return the original response structure
    response = api_request(url=url, params=params, headers=headers)
    return response


def get_unit_details(*, config, unit_id):
    """
    Get detailed information about a specific unit, including questions.

    Args:
        config (dict): The connector configuration
        unit_id (str): The ID of the unit to fetch

    Returns:
        dict: The API response containing unit details
    """
    base_url = get_base_url(config)
    headers = get_auth_headers(config)

    url = f"{base_url}/v1/units/{unit_id}/"
    return api_request(url=url, params={}, headers=headers)


def get_unit_attempts(*, config, unit_id, limit=100, updated_after=None):
    """
    Get all attempts for a quiz with pagination support.

    Args:
        config (dict): The connector configuration
        unit_id (str): The ID of the unit to fetch attempts for
        limit (int): Maximum number of attempts to return per page
        updated_after (str): ISO timestamp to filter attempts updated after this time

    Returns:
        list: All attempts for the unit
    """
    base_url = get_base_url(config)
    headers = get_auth_headers(config)

    params = {
        "unit_id": unit_id,
        "limit": limit,
    }

    # Add updated_after filter if provided
    if updated_after:
        params["updated_after"] = updated_after

    url = f"{base_url}/v1/user-progress/"

    # Use paginated request to get all attempts
    attempts = paginated_api_request(
        url=url,
        params=params,
        headers=headers,
        data_path=["data"],  # Adjust based on actual response structure
        limit=limit,
    )

    return attempts


def get_question_attempts(*, config, user_id, unit_id, attempt_num):
    """
    Get all question attempts for a specific quiz attempt.

    Args:
        config (dict): The connector configuration
        user_id (str): The ID of the user
        unit_id (str): The ID of the unit
        attempt_num (int): The attempt number

    Returns:
        dict: The API response containing question attempt information
    """
    base_url = get_base_url(config)
    headers = get_auth_headers(config)

    params = {
        "user_id": user_id,
        "unit_id": unit_id,
        "attempt_num": attempt_num,
    }
    url = f"{base_url}/v1/user-responses/"
    return api_request(url=url, params=params, headers=headers)
