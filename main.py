import functions_framework
import json
from google.cloud import storage
import pandas as pd
import re 

# Global variables
BUCKET_NAME = 'gadoe_data'
FILE_NAME = 'gshs/GSHS_2024_{survey_type}_629.xlsx'
cached_data = None

survey_types = ['ES', 'MSHS', 'Parents', 'Personnel']

def get_data_from_storage():
    global cached_data

    if cached_data is not None:
        print("Using cached data")
        return cached_data

    try:
        print("Reading data from storage")

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        cached_data = {} 
        for survey_type in survey_types:
            print(f"loading survey type: {survey_type}")
            file = FILE_NAME.format(survey_type=survey_type)

            blob = bucket.blob(file)
            contents = blob.download_as_bytes()

            cached_data[survey_type] = pd.read_excel(contents)  # Use pandas to parse Excel

    except Exception as e:
        print(f"Error reading from storage: {e}")
        return "Internal Server Error", 500
    finally:
        return cached_data

def list_questions(survey_data, parms):
    print(f"listing questions: ")

    survey_type = parms['survey_type']
    data = survey_data[survey_type]

    pattern = r'^\d+\. .+'
    for index, string in enumerate(list(data.columns)):
        if re.match(pattern, string):
            return list(survey_data[survey_type].columns)[index:]
    
    return list(survey_data[survey_type].columns)[10:]

def tabulate_question(data, parms):
    print(f"tabulating question: {parms['question']}")

    try: 
        survey_data = data[parms['survey_type']]
        print(f"{survey_data=}")
        question = parms['question']
        list_of_questions = list_questions(data, parms)

        for idx, q in enumerate(list_of_questions):
            if question in q:
                question = q
                print(f"found question: {q}")
                break

        # question = "5. I get along with other students at school."

        by_category = parms['by_category']

        if by_category == 'School':
            category = ' SchoolName'
            all_category = 'All Schools'
        elif by_category == 'Ethnicity':
            category = 'Ethnicity'
            all_category = 'All Ethnicities'
        elif by_category == 'Gender':
            category = 'Gender'
            all_category = 'All Genders'
        else: 
            category = ' SchoolName'

        print(f"tabulating data for question: {question} and category: {category}")
        #crosstable to get the counts 
        transformed_df = pd.crosstab(survey_data[category], survey_data[question])
        print(transformed_df.head())
        transformed_df.reset_index(inplace=True)
        transformed_df.columns.name = None

        # Calculate the sums of the response columns 
        column_names = [str(col) for col in transformed_df.columns[1:]]
        sums = transformed_df[column_names].sum()

        # Create a new row with the sums
        all_category_row = pd.DataFrame([[all_category] + sums.tolist()], columns=transformed_df.columns)

        # Append the new row to the original DataFrame
        df_with_all_categories = pd.concat([all_category_row, transformed_df], ignore_index=True)

        # Add a 'Totals' column
        df_with_all_categories['Totals'] = df_with_all_categories[column_names].sum(axis=1)
    except Exception as e:
        print(f"Error tabulating question: {e}")
        return "Internal Server Error", 500
    finally:
        return df_with_all_categories

@functions_framework.http
def service(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    print("servicing request ... ")
    request_json = request.get_json(silent=True)
    request_args = request.args
    path = request.path
    print(f"request path: {path}")

    print(request_json)

    data = get_data_from_storage()

    # print(data.columns)

    try: 
        name = None 

        if request_json and 'name' in request_json:
            name = request_json['name']
        elif request_args and 'name' in request_args:
            name = request_args['name']
        elif request_json and 'action' in request_json:
            print(f"the action is: {request_json['action']}")

            if request_json['action'] == 'tabulate':
                print("in tabulate action ... ")
                df = tabulate_question(data, request_json)
                print(f"the length of the tabulated dataframe: {df.index.size}")
                print(json.dumps(df.to_json(orient='records'))) 
                return json.dumps({'data_table': df.to_json(orient='records')})

            elif request_json['action'] == 'list':
                print("in list action ... ")
                questions = list_questions(data, request_json)
                print(f"the length of the questions: {len(questions)}")
                print(json.dumps(questions)) 
                return json.dumps({'questions': questions})

        else:
            name = 'World'
        
        return json.dumps({'message': f'Hello {name}!'})
    except Exception as e:
        print(f"Error processing request: {e}")
        return json.dumps({'error': str(e)})