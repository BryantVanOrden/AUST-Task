
import requests
import json

# =================================================================
# CONFIGURATION: NO EXCUSES. FILL IN YOUR DETAILS.
# =================================================================
MOODLE_URL = "http://localhost"  # USE HTTP, NOT HTTPS FOR LOCALHOST
MOODLE_TOKEN = "849db9d501d22586717b104973e74b7d" # YOUR MOODLE WEB SERVICE TOKEN

# The single endpoint for all Moodle API calls
API_ENDPOINT = f"{MOODLE_URL}/webservice/rest/server.php"


class MoodleClient:
    """
    This is your weapon. It executes commands with precision.
    Each method is a different combat technique.
    """
    def __init__(self, endpoint, token):
        if not endpoint or not token:
            raise ValueError("Endpoint and token cannot be empty. Discipline!")
        self.endpoint = endpoint
        self.token = token
        self.default_params = {
            'wstoken': self.token,
            'moodlewsrestformat': 'json',
        }

    def _make_request(self, function_name, params=None):
        """
        THE CORE ENGINE. This is where the power is.
        It takes the command and executes the strike.
        """
        payload = self.default_params.copy()
        payload['wsfunction'] = function_name

        # Merge function-specific parameters
        if params:
            payload.update(params)

        print(f"Executing strike: {function_name}...")
        try:
            response = requests.post(self.endpoint, data=payload)
            response.raise_for_status()  # Raises an exception for bad status codes (4xx or 5xx)
            
            # Moodle can return 200 OK but still have an error in the body
            json_response = response.json()
            if isinstance(json_response, dict) and 'exception' in json_response:
                print(f"---! API ERROR !---")
                print(f"Error Code: {json_response.get('errorcode')}")
                print(f"Message: {json_response.get('message')}")
                return None

            print("--- ✓ SUCCESS ---")
            return json_response

        except requests.exceptions.RequestException as e:
            print(f"---! HTTP REQUEST FAILED !---")
            print(f"Error: {e}")
            return None
        except json.JSONDecodeError:
            print("---! FAILED TO DECODE RESPONSE !---")
            print(f"Received non-JSON response: {response.text}")
            return None

    # --- USER MANAGEMENT TECHNIQUES ---

    def create_users(self, users_data):
        """
        CREATE: Forge new warriors.
        `users_data` must be a list of dictionaries.
        Each dictionary is a new user profile.
        """
        params = {}
        for i, user in enumerate(users_data):
            for key, value in user.items():
                params[f'users[{i}][{key}]'] = value
        
        return self._make_request('core_user_create_users', params)

    def update_users(self, users_data):
        """
        UPDATE: Reforge existing warriors.
        `users_data` must be a list of dictionaries.
        Each dictionary MUST include the user 'id'.
        """
        params = {}
        for i, user in enumerate(users_data):
            for key, value in user.items():
                params[f'users[{i}][{key}]'] = value
        
        return self._make_request('core_user_update_users', params)

    def delete_users(self, user_ids):
        """
        DELETE: Banish warriors from the roster.
        `user_ids` is a list of user integer IDs.
        """
        params = {}
        for i, user_id in enumerate(user_ids):
            params[f'userids[{i}]'] = user_id
            
        return self._make_request('core_user_delete_users', params)

    def get_users_by_field(self, field, values):
        """
        SEARCH: Find your warriors.
        `field`: The field to search for (e.g., 'username', 'email', 'id').
        `values`: A list of values for that field.
        """
        params = {'field': field}
        for i, value in enumerate(values):
            params[f'values[{i}]'] = value
        
        return self._make_request('core_user_get_users_by_field', params)


# =================================================================
# THE TRAINING GROUND: This is where you practice your moves.
# Uncomment the blocks to test each function.
# =================================================================
if __name__ == "__main__":
    # 1. Prepare your weapon
    client = MoodleClient(API_ENDPOINT, MOODLE_TOKEN)
    print(f"Moodle Client Initialized for {MOODLE_URL}\n")

    # --- Practice Drill 1: CREATE a new user ---
    new_user_to_create = [
        {
            'username': 'testwarrior01',
            'password': 'Password123!',
            'firstname': 'Test',
            'lastname': 'Warrior',
            'email': 'test.warrior01@example.com',
            'auth': 'manual' # Use 'manual' for standard accounts
        }
    ]
    created_user_info = client.create_users(new_user_to_create)
    
    if created_user_info:
        print("User Created Successfully:")
        print(json.dumps(created_user_info, indent=2))
        
        # We need the new user's ID for the next steps
        new_user_id = created_user_info[0]['id']

        # --- Practice Drill 2: UPDATE the user's details ---
        print("\n--- Updating User ---")
        user_to_update = [
            {
                'id': new_user_id,
                'firstname': 'UpdatedFirstname',
                'lastname': 'UpdatedLastname'
            }
        ]
        client.update_users(user_to_update)
        
        # --- Practice Drill 3: VERIFY the update ---
        print("\n--- Verifying Update ---")
        updated_user = client.get_users_by_field(field='id', values=[new_user_id])
        if updated_user:
            print(json.dumps(updated_user, indent=2))
        
        # --- Practice Drill 4: DELETE the user ---
        print(f"\n--- Deleting User ID: {new_user_id} ---")
        client.delete_users([new_user_id])
        
        # --- Final Drill: CONFIRM deletion ---
        print("\n--- Confirming Deletion ---")
        deleted_user_check = client.get_users_by_field(field='id', values=[new_user_id])
        if deleted_user_check == []:
            print("Confirmation: User has been successfully deleted.")
        else:
            print("Warning: User was not deleted.")
            print(json.dumps(deleted_user_check, indent=2))