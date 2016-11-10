from taskbuffer import GlobalShares
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec

if __name__ == "__main__":
    """
    Functional testing of the shares tree
    """
    global_shares = GlobalShares()

    # print the global share structure
    print('--------------GLOBAL SHARES TREE---------------')
    print(global_shares.tree)

    # print the normalized leaves, which will be the actual applied shares
    print('--------------LEAVE SHARES---------------')
    print(global_shares.leave_shares)

    # print the shares in order of under usage
    print('--------------LEAVE SHARES SORTED BY UNDER-PLEDGING---------------')
    print global_shares.get_sorted_leaves()

    # check a couple of shares if they are valid leave names
    share_name = 'wrong_share'
    print ("Share {0} is valid: {1}".format(share_name, global_shares.is_valid_share(share_name)))
    share_name = 'MC16Pile'
    print ("Share {0} is valid: {1}".format(share_name, global_shares.is_valid_share(share_name)))

    # create a fake tasks with relevant fields and retrieve its share
    task_spec = JediTaskSpec()

    # Analysis task
    task_spec.prodSourceLabel = 'user'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'dummy_wg'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Analysis')".format(global_shares.get_share_for_task(task_spec)))

    # Production task without any matching leave
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'dummy_wg'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Undefined')".format(global_shares.get_share_for_task(task_spec)))

    # Test task
    task_spec.prodSourceLabel = 'test123'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'dummy_wg'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Test')".format(global_shares.get_share_for_task(task_spec)))

    # Derivations task without any matching leave
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'GP_PHYS'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Undefined')".format(global_shares.get_share_for_task(task_spec)))

    # Reprocessing task without any matching leave
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'AP_REPR'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Undefined')".format(global_shares.get_share_for_task(task_spec)))

    # Group production task
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'GP_LOL'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Group production')".format(global_shares.get_share_for_task(task_spec)))

    # Upgrade task
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'AP_UPG'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Upgrade')".format(global_shares.get_share_for_task(task_spec)))

    # HLT Reprocessing
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'AP_THLT'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'HLT Reprocessing')".format(global_shares.get_share_for_task(task_spec)))

    # Validation
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'AP_VALI'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Validation')".format(global_shares.get_share_for_task(task_spec)))

    # Event Index
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'proj-evind'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Event Index')".format(global_shares.get_share_for_task(task_spec)))

    # MC Derivations
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'mc.*'
    task_spec.workingGroup = 'GP_PHYS'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'MC Derivations')".format(global_shares.get_share_for_task(task_spec)))

    # Data Derivations
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'data.*'
    task_spec.workingGroup = 'GP_PHYS'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Data Derivations')".format(global_shares.get_share_for_task(task_spec)))