# List of methods that can be executed by the clients

allowed_methods = []

# methods from pandaserver.taskbuffer.Utils
allowed_methods += [
    "putEventPickingRequest",
    "put_checkpoint",
    "delete_checkpoint",
    "put_file_recovery_request",
    "put_workflow_request",
]

# methods from pandaserver.userinterface.UserIF
allowed_methods += [
    "insertTaskParams",
    "setNumSlotsForWP",
    "relay_idds_command",
    "execute_idds_workflow_command",
]
