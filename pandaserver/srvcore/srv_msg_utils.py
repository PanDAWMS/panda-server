import json
import datetime


# make message
def make_message(msg_type, **kwargs):
    msg_dict = {'msg_type': msg_type}
    msg_dict.update(kwargs)
    msg_dict['timestamp'] = int(datetime.datetime.utcnow().timestamp())
    return json.dumps(msg_dict)


# send a job message
def send_job_message(msg_queue, msg_topic, task_id, job_id):
    # make message
    msg = make_message('get_job', taskid=task_id, jobid=job_id)
    # use job ID for selector
    headers = {'type': job_id}
    # send message to topic first
    msg_topic.send(msg)
    # send the same message to queue
    msg_queue.send(msg, headers=headers)


# delete a job message
def delete_job_message(msg_queue, job_id, time_out=10):
    # job ID for selector
    headers = {'selector': "type='{0}' OR JMSType='{0}'".format(job_id)}
    # subscribe to remove job messages
    msg_queue.add_remover(headers, time_out)
    # delete old removers
    msg_queue.purge_removers()


# send a task message
def send_task_message(msg_topic, command_str, task_id):
    # make message
    msg = make_message('{}_task'.format(command_str), taskid=task_id)
    # send message to topic
    msg_topic.send(msg)
