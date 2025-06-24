from pathlib import Path
from mccode_antlr.instr import Instr
from mccode_plumber.kafka import register_kafka_topics, all_exist
from mccode_plumber.splitrun import make_parser

def guess_instr_config(name: str):
    from os import access, R_OK
    guess = f'/event-formation-unit/configs/{name}/configs/{name}.json'
    if access(guess, R_OK):
        return guess
    raise FileNotFoundError(f'Could not find {guess} in {name}')

def guess_instr_calibration(name: str):
    from os import access, R_OK
    guess = f'/event-formation-unit/configs/{name}/configs/{name}nullcalib.json'
    if access(guess, R_OK):
        return guess
    raise FileNotFoundError(f'Could not find {guess} in {name}')


def get_topics(data: dict):
    topics = set()
    for k, v in data.items():
        if isinstance(v, dict):
            topics.update(get_topics(v))
        elif k == 'topic':
            topics.add(v)
    return topics


def conduct(instr: Instr, args, dump_structure=False):
    from datetime import datetime, timezone
    from mccode_plumber.epics import convert_instr_parameters_to_nt
    from .efu import EventFormationUnitManager
    from .epics import EPICSMailboxManager
    from .forwarder import ForwarderManager
    from .writer import WriterManager

    now = datetime.now(timezone.utc).isoformat(timespec='seconds').split('+')[0]
    params = {
        'broker': 'localhost:9092',
        'work': Path().resolve(),
        'config': 'ForwardConfig',
        'status': 'ForwardStatus',
        'prefix': 'mcstas:',
        'command': 'WriterCommand',
        'job': 'WriterJob',
        'event_source': f'{instr.name}_detector',
        'event_topic': 'SimulatedEvents',
        'parameter_topic': 'SimulatedParameters',
        'title': f'{instr.name} simulation {now}: {args}',
        'filename': f'{instr.name}_{now}.h5',
        'origin': 'sample_stack',
        'monitor_source': 'mccode-to-kafka',
    }
    if (structure := params['work'] / f'{instr.name}.json').exists():
        params['structure_file'] = structure
    if dump_structure:
        params['structure_dump'] = params['work'] / f'{instr.name}.dump.json'

    # Start up services
    efu = EventFormationUnitManager.start(
        binary=instr.name,
        config=args.config or guess_instr_config(name=instr.name),
        calibration=args.calibration or guess_instr_calibration(name=instr.name),
        broker=params['broker'],
        topic=params['event_topic'],
    )
    forwarder = ForwarderManager.start(
        broker=params['broker'],
        config=params['config'],
        status=params['status'],
    )
    epics = EPICSMailboxManager.start(
        values=convert_instr_parameters_to_nt(instr.parameters),
        prefix=params['prefix'],
    )
    writer = WriterManager.start(
        broker=params['broker'],
        work=params['work'],
        command=params['command'],
        jbo=params['job'],
    )

    # Configure topics:
    to_register = [params['parameter_topic'], params['event_topic']]
    # plus the da00 topics listed in the nexus structure json file:
    if 'structure_file' in params:
        from json import load
        with open(params['structure_file']) as f:
            topics = get_topics(load(f))
        to_register.extend(topics)

    res = register_kafka_topics(params['broker'], to_register)
    if not all_exist(res.values()):
        raise RuntimeError(f'Missing Kafka topics? {res}')

    # Tell the forwarder what to forward
    # FIXME mp-forwarder-setup instr --prefix epics_prefix --config broker/config --topic parameter_topic

    # Create a file-writer job
    # FIXME mp-writer-write instr --prefix epicx_prefix --broker broker --job job
    #       --command command --topic parameter_topic --event-source event_source
    #       --event-topic event-topic --title title --filename filename
    #       --ns-file structure_File --start-time {now} --origin origin --job-id {uuid}

    ### Do the actual simulation
    # FIXME call main in mccode_plumber.splitrun

    ### Wait for the file-writer to finish its job (possibly kill it)

    ### De-register the forwarder topics (Don't bother since we're about to kill it)

    ### Stop the services
    epics.stop()
    writer.stop()
    forwarder.stop()
    efu.stop()

    ### Repack the resulting file into contiguous memory
    ## Equivalent to `h5repack -l CONTI ${filename} ${filename}.pack && mv ${filename}.pack ${filename}`

    ### Insert the HDF5 representation of the instrument into the file, just incase