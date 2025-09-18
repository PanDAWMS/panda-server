jedi_task_plain = """
Subject: JEDI notification for TaskID:{jedi_task_id} ({n_succeeded}/{n_total} {msg_succeeded})

Summary of TaskID:{jedi_task_id}

Created : {creation_time} (UTC)
Ended   : {end_time} (UTC)

Final Status : {task_status}

Total Number of {input_str}   : {n_total}
             Succeeded   : {n_succeeded}
             Failed      : {n_failed}
             {cancelled_str} : {n_cancelled}


Error Dialog : {error_dialog}

{dataset_summary}

Parameters : {command}


PandaMonURL : https://bigpanda.cern.ch/task/{jedi_task_id}/

Please use the smiley interface on this link to provide feedback on the task execution. 

Estimated carbon footprint for the task
     Succeeded   : {carbon_succeeded}
     Failed      : {carbon_failed}
     Cancelled   : {carbon_cancelled}
     _________________
     Total       : {carbon_total}
(More details on estimation: https://panda-wms.readthedocs.io/en/latest/advanced/carbon_footprint.html)


Report Panda problems of any sort to

  the eGroup for help request
    hn-atlas-dist-analysis-help@cern.ch
    
  the Discourse forum for distributed computing help  
    https://atlas-talk.web.cern.ch/c/distributed-computing-help

  the JIRA portal for software bug
    https://its.cern.ch/jira/browse/ATLASPANDA
"""

html_head = """
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        .logo {{
            width: 100px; /* Adjust the size as needed */
        }}
    </style>
    <title> {title} </title>
</head>
"""

jedi_task_html_body = """
<body>
    <img class="logo" src="https://panda-wms.readthedocs.io/en/latest/_static/PanDA-rev-logo-small-200px.jpg" alt="PanDA Logo">
    <h2>Summary of TaskID: <a href="https://bigpanda.cern.ch/task/{jedi_task_id}">{jedi_task_id}</a> </h2>
    <p> 💬 Please use the smiley interface on this link to provide feedback on the task execution.</p>
    <table>
        <tr>
            <th>Detail</th>
            <th>Value</th>
        </tr>
        <tr>
            <td>Created</td>
            <td>{creation_time}</td>
        </tr>
        <tr>
            <td>Ended</td>
            <td>{end_time}</td>
        </tr>
        <tr>
            <td>Final Status</td>
            <td>{task_status}</td>
        </tr>
    </table>

    <h3>Total Number of {input_str}</h3>
    <table>
        <tr>
            <th>Category</th>
            <th>Count</th>
        </tr>
        <tr>
            <td>Succeeded</td>
            <td>{n_succeeded}</td>
        </tr>
        <tr>
            <td>Failed</td>
            <td>{n_failed}</td>
        </tr>
        <tr>
            <td>{cancelled_str}</td>
            <td>{n_cancelled}</td>
        </tr>
        <tr>
            <td>Total</td>
            <td>{n_total}</td>
        </tr>
    </table>

    <p><strong>Error Dialog:</strong> {error_dialog}</p>

    <h3>Datasets</h3>
    <table>
        <tr>
            <td><strong>In</strong></td>
            <td>{datasets_in}</td>
        </tr>
        <tr>
            <td><strong>Out</strong></td>
            <td>{datasets_out}</td>
        </tr>
        <tr>
            <td><strong>Log</strong></td>
            <td>{datasets_log}</td>
        </tr>
    </table>

    <h3>Parameters</h3>
    <table>
        <tr>
            <td><strong>Command</strong></td>
            <td>{command}</td>
        </tr>
    </table>

    <h3>Estimated Carbon Footprint for the Task</h3>
    <table>
        <tr>
            <th>Category</th>
            <th>gCO2</th>
        </tr>
        <tr>
            <td>Succeeded</td>
            <td>{carbon_succeeded}</td>
        </tr>
        <tr>
            <td>Failed</td>
            <td>{carbon_failed}</td>
        </tr>
        <tr>
            <td>Cancelled</td>
            <td>{carbon_cancelled}</td>
        </tr>
        <tr>
            <td><strong>Total</strong></td>
            <td><strong>{carbon_total}</strong></td>
        </tr>
    </table>
    <p>More details on estimation: <a href="https://panda-wms.readthedocs.io/en/latest/advanced/carbon_footprint.html">https://panda-wms.readthedocs.io/en/latest/advanced/carbon_footprint.html</a></p>

    <h3>Panda Problem Reporting</h3>
    <ul>
        <li>The eGroup for help request: <a href="mailto:hn-atlas-dist-analysis-help@cern.ch">hn-atlas-dist-analysis-help@cern.ch</a></li>
        <li>The Discourse forum for distributed computing help: <a href="https://atlas-talk.web.cern.ch/c/distributed-computing-help">https://atlas-talk.web.cern.ch/c/distributed-computing-help</a></li>
        <li>The JIRA portal for software bug: <a href="https://its.cern.ch/jira/browse/ATLASPANDA">https://its.cern.ch/jira/browse/ATLASPANDA</a></li>
    </ul>
</body>
</html>
"""
