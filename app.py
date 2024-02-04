import boto3
import json

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

# Specify your DynamoDB table name
table_name = 'Skoolbook_data_common'
table = dynamodb.Table(table_name)

# Load JSON data from file
with open('chapters_data.json', 'r') as json_file:
    json_data = json.load(json_file)


    
for class_number, subjects_data in json_data['chapters'].items():
    class_key = f'C{class_number}'  # Use 'C' + class_number as a string

    for subject, chapters in subjects_data.items():
        subject_key = subject

        # Prepare update expression and attribute values
        update_expression = f"SET #class.#subject = list_append(if_not_exists(#class.#subject, :emptyList), :chapters)"
        expression_attribute_values = {
            ':emptyList': {'L': []},
            ':chapters': {'L': chapters}
        }

        # Perform UpdateItem operation
        try:
            response = table.update_item(
                Key={
                    'data_segment': 'chapters',
                     class_key: 'M'
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values
            )
            print(f"UpdateItem succeeded for Class {class_number}, Subject {subject}: {response}")
        except Exception as e:
            print(f"Error updating item for Class {class_number}, Subject {subject}: {e}")