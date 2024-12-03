import ast
import copy
import re
import traceback

import yaml
from docstring_parser import parse

EXCLUDED_FUNCTIONS = ["init_task_buffer"]
EXCLUDED_PARAMS = ["req"]

DEFAULT_RESPONSE_TEMPLATE = {
    "200": {
        "description": "Method called correctly",
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "success": {"type": "boolean", "example": True, "description": "Indicates whether the request was successful (True) or not (False)"},
                        "message": {
                            "type": "string",
                            "description": "Message indicating the nature of the failure. Empty or meaningless if the request was successful.",
                        },
                        "response": {
                            "type": "object",
                            "description": "The data returned if the operation is successful. Null if it fails or the method does not generate return data.",
                        },
                    },
                    "required": ["success", "message", "response"],
                }
            }
        },
    },
    "403": {
        "description": "Forbidden",
        "content": {"text/plain": {"schema": {"type": "string", "example": "You are calling an undefined method is not allowed for the requested URL"}}},
    },
    "404": {"description": "Not Found", "content": {"text/plain": {"schema": {"type": "string", "example": "Resource not found"}}}},
    "500": {
        "description": "INTERNAL SERVER ERROR",
        "content": {
            "text/plain": {
                "schema": {
                    "type": "string",
                    "example": "INTERNAL SERVER ERROR. The server encountered an internal error and was unable to complete your request.",
                }
            }
        },
    },
}


def extract_docstrings(file_path):
    """
    Extract all docstrings from a Python file.

    Args:
        file_path (str): Path to the Python file.

    Returns:
        dict: A dictionary where keys are function/class/module names and values are docstrings.
    """
    with open(file_path, "r") as file:
        tree = ast.parse(file.read())

    docstrings = {}

    # Extract module-level docstring
    if ast.get_docstring(tree):
        docstrings["__module__"] = ast.get_docstring(tree)

    # Walk through all nodes in the file
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name not in EXCLUDED_FUNCTIONS:
            # Get the name of the function/class/module
            name = node.name if hasattr(node, "name") else "__module__"
            # Get the docstring
            docstring = ast.get_docstring(node)
            if docstring:
                docstrings[name] = docstring

    return docstrings


def extract_parameters(parsed):
    """
    Extract parameters from a docstring and format them as OpenAPI parameters.
    """
    parameters = []

    # Iterate over the parameters in the docstring
    # print(f"Parameters: {parsed.params}")
    for param in parsed.params:
        # Skip excluded parameters that we don't want to appear in the API documentation
        if param.arg_name in EXCLUDED_PARAMS:
            continue

        # Map each parameter to OpenAPI format
        param_type = "string"  # Default type (fallback)
        if "list" in param.type_name:
            param_type = "array"
        elif "int" in param.type_name:
            param_type = "integer"
        elif "float" in param.type_name:
            param_type = "number"
        elif "bool" in param.type_name:
            param_type = "boolean"

        print(f"Param: {param.arg_name}, Type: {param_type}, Optional: {param.is_optional} {param.description}")
        is_required = not param.is_optional

        parameter_schema = {"name": param.arg_name, "in": "query", "required": is_required, "schema": {"type": param_type}, "description": param.description}

        if param_type == "array":
            parameter_schema["schema"]["items"] = {"type": "string"}  # Default array item type

        parameters.append(parameter_schema)

    return parameters


def extract_parameters_as_json(parsed):
    """
    Extract parameters from a docstring and format them as JSON schema for OpenAPI.
    """
    properties = {}
    required_parameters = []

    print(f"Parameters(json): {parsed.params}")
    for param in parsed.params:
        # Skip excluded parameters that we don't want to appear in the API documentation
        if param.arg_name in EXCLUDED_PARAMS:
            continue

        # Map the type to OpenAPI type
        param_type = "string"  # Default type
        if "list" in param.type_name:
            param_type = "array"
        elif "int" in param.type_name:
            param_type = "integer"
        elif "float" in param.type_name:
            param_type = "number"
        elif "bool" in param.type_name:
            param_type = "boolean"

        # Build schema for the parameter
        param_schema = {"type": param_type, "description": param.description}

        if param_type == "array":
            param_schema["items"] = {"type": "object"}  # Default array item type

        properties[param.arg_name] = param_schema

        if not param.is_optional:
            required_parameters.append(param.arg_name)

    return {"type": "object", "properties": properties, "required": required_parameters}


def get_custom_metadata(docstring):
    # Parse the docstring manually for the custom section
    doc_lines = docstring.splitlines()
    custom_section_name = "API details:"
    custom_sections = {}
    inside_custom_section = False

    for line in doc_lines:
        stripped_line = line.strip()
        if stripped_line == custom_section_name:
            inside_custom_section = True
            continue  # Skip the section header line
        if inside_custom_section:
            if stripped_line == "" or ":" not in stripped_line:  # End of custom section
                break
            key, value = map(str.strip, stripped_line.split(":", 1))
            custom_sections[key] = value

    return custom_sections


def convert_docstrings_to_openapi(docstrings):
    """
    Convert extracted docstrings to OpenAPI format.

    Args:
        docstrings (dict): A dictionary where keys are function/class/module names and values are docstrings.

    Returns:
        dict: A dictionary where keys are function/class/module names and values are OpenAPI descriptions.
    """

    # Initialize the OpenAPI dictionary
    openapi = {"openapi": "3.0.3", "info": {"title": "PanDA API", "version": "1.0.0"}, "paths": {}}

    for name, docstring in docstrings.items():
        try:
            parsed_docstring = parse(docstring)
            summary = parsed_docstring.short_description

            # Remove the custom "API details" section from the long description
            description = re.sub(r"API details:\n(?:\s+.*\n?)*", "", parsed_docstring.long_description)

            custom_metadata = get_custom_metadata(docstring)
            method = custom_metadata.get("HTTP Method", "POST").lower()
            path = custom_metadata.get("Path", name)

            openapi["paths"][path] = {}
            openapi["paths"][path][method] = {"summary": summary, "description": description}

            # Extract parameters from the docstring
            parameters = extract_parameters(parsed_docstring)
            if method in ["post", "put"]:
                request_body_schema = extract_parameters_as_json(parsed_docstring)
                openapi["paths"][path][method]["requestBody"] = {"required": True, "content": {"application/json": {"schema": request_body_schema}}}
            elif method in ["get", "delete"]:
                openapi["paths"][path][method]["parameters"] = parameters

            if parsed_docstring.returns:
                return_type = parsed_docstring.returns.type_name
                return_desc = parsed_docstring.returns.description

            # Add default responses
            openapi["paths"][path][method]["responses"] = copy.deepcopy(DEFAULT_RESPONSE_TEMPLATE)

        except (AttributeError, TypeError):
            print(f"Docstring for {name} could not be parsed. Failed with error:\n{traceback.format_exc()}")

    return openapi


if __name__ == "__main__":
    # Define the path to the Python file to convert
    file_path = "v1/harvester_api.py"
    docstrings = extract_docstrings(file_path)

    # Convert docstrings to OpenAPI
    open_api_doc = convert_docstrings_to_openapi(docstrings)
    with open("/tmp/panda_api.yaml", "w") as output_file:
        yaml.dump(open_api_doc, output_file, sort_keys=False)
