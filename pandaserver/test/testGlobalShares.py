import sys

from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

if __name__ == "__main__":
    """
    Functional testing of the shares tree
    """
    proxyS = DBProxy()
    proxyS.connect(
        panda_config.dbhost,
        panda_config.dbpasswd,
        panda_config.dbuser,
        panda_config.dbname,
    )

    # print the shares in order of under usage
    print("--------------LEAVE SHARES SORTED BY UNDER-PLEDGING---------------")
    print(proxyS.get_sorted_leaves())

    # print the global share structure
    print("--------------GLOBAL SHARES TREE---------------")
    print(proxyS.tree)

    # print the normalized leaves, which will be the actual applied shares
    print("--------------LEAVE SHARES---------------")
    print(proxyS.leave_shares)

    # print the current grid status
    print("--------------CURRENT GRID STATUS---------------")
    print(proxyS.tree.pretty_print_hs_distribution(proxyS._DBProxy__hs_distribution))

    # check a couple of shares if they are valid leave names
    share_name = "wrong_share"
    print(f"Share {share_name} is valid: {proxyS.is_valid_share(share_name)}")
    share_name = "MC16Pile"
    print(f"Share {share_name} is valid: {proxyS.is_valid_share(share_name)}")

    try:
        from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
    except ImportError:
        print("Skipped task tests since JEDI module depency not satisfied")
        sys.exit(0)

    # create a fake tasks with relevant fields and retrieve its share
    task_spec = JediTaskSpec()

    # Analysis task
    task_spec.prodSourceLabel = "user"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "dummy_wg"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Analysis')")

    # Production task without any matching leave
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "dummy_wg"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Undefined')")

    # Test task
    task_spec.prodSourceLabel = "test123"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "dummy_wg"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Test')")

    # Derivations task without any matching leave
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "GP_PHYS"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Undefined')")

    # Reprocessing task without any matching leave
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "AP_REPR"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Undefined')")

    # Group production task
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "GP_LOL"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Group production')")

    # Upgrade task
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "AP_UPG"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Upgrade')")

    # HLT Reprocessing
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "AP_THLT"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'HLT Reprocessing')")

    # Validation
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "AP_VALI"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Validation')")

    # Event Index
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "dummy_campaign"
    task_spec.workingGroup = "proj-evind"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Event Index')")

    # MC Derivations
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "mc.*"
    task_spec.workingGroup = "GP_PHYS"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'MC Derivations')")

    # Data Derivations
    task_spec.prodSourceLabel = "managed"
    task_spec.campaign = "data.*"
    task_spec.workingGroup = "GP_PHYS"
    task_spec.processingType = "dummy_type"
    print(f"Share for task is {proxyS.get_share_for_task(task_spec)}(should be 'Data Derivations')")
