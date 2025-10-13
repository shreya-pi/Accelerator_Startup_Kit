
import json
import re

def format_api_response(data):
    try:
        data = json.loads(data)
        data = data['choices'][0]['messages']
        
        # Replace the literal two-character sequence '\\n\\n' with two actual newlines
        formatted_message = data.replace('\\n\\n', '\n\n')
        
        # Now, replace any remaining literal '\\n' sequences with a single newline
        formatted_message = formatted_message.replace('\\n', '\n')
        

        return formatted_message

    except (json.JSONDecodeError, KeyError, IndexError) as e:
        return f"Error processing the response: {e}"


with open('Data_Duplication/input_and_output_files/views.json', 'r', encoding='utf-8') as file:
    raw_response = file.read()

# Call the function with the raw response
clean_output = format_api_response(raw_response)

# Print the final result
# print(clean_output)
with open('Data_Duplication/input_and_output_files/formatted_views.md', 'w', encoding='utf-8') as file:
    file.write(clean_output)
    print("Formatted response saved!")