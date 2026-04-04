import pkgutil
import importlib


def discover_processors():
    """Return all *Processor classes found in modules directly under ecosystem_services/.

    Any class whose name ends in 'Processor' in any module at this package level
    is picked up automatically — no changes to orchestrating scripts required when
    adding a new ecosystem service module.
    """
    import ecosystem_services

    procs = []
    for _, name, _ in pkgutil.iter_modules(ecosystem_services.__path__):
        mod = importlib.import_module(f"ecosystem_services.{name}")
        for attr in vars(mod).values():
            if isinstance(attr, type) and attr.__name__.endswith("Processor"):
                procs.append(attr)
    return procs
