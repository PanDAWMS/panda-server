__author__ = 'retmas'

import logging, sys, os, json
from snakeparser import Parser
from pandaserver.workflow.pcwl_utils import resolve_nodes
from pandaserver.workflow.workflow_utils import get_node_id_map, dump_nodes, set_workflow_outputs, \
    convert_nodes_to_workflow

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)


def verify_node(node):
    result, error = node.verify()
    if not result:
        logging.info(f'Verification error in ID {node.id}: {error}')


# noinspection PyBroadException
def main():
    try:
        workflow_file = sys.argv[1]
        data = dict()
        logging.info(f'{os.path.basename(__file__)}: workflow_file = {workflow_file}')
        parser = Parser(workflow_file, level=logging.DEBUG)
        logging.info(f'verify_workflow = {parser.verify_workflow()}')
        nodes, root_in = parser.parse_nodes()
        _ = parser.parse_code()
        dot_data = parser.get_dot_data()
        logging.info(f'dot data ={os.linesep}{dot_data}')
        s_id, t_nodes, nodes = resolve_nodes(nodes, root_in, data, 0, set(), sys.argv[2], logging)
        set_workflow_outputs(nodes)
        id_map = get_node_id_map(nodes)
        task_template = None
        with open(os.path.join(os.path.dirname(__file__), 'psnakemake_task.json'), 'r') as task_fp:
            with open(os.path.join(os.path.dirname(__file__), 'psnakemake_container.json'), 'r') as container_fp:
                task_template = {'athena': json.load(task_fp), 'container': json.load(container_fp)}
        if not task_template:
            raise Exception('Failed to load task template')
        _ = list(map(lambda o: o.resolve_params(task_template, id_map), nodes))
        logging.info(dump_nodes(nodes))
        workflow, dump_str_list = convert_nodes_to_workflow(nodes)
        logging.info(''.join(dump_str_list))
        _ = list(map(lambda o: verify_node(o), nodes))
    except Exception as ex:
        logging.error(f'exception occurred: {ex}', exc_info=True)


if __name__ == '__main__':
    main()
