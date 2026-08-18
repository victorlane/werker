__version__ = "0.1.0.dev0"

# Deliberately no eager imports here: this module is imported very early
# during Django's app loading (AppConfig.create -> import_module(entry)),
# before the app registry is ready — importing werker.decorators (which
# reaches werker.models) at this point raises AppRegistryNotReady. Import
# the public API from its actual submodules instead, e.g.
# `from werker.decorators import at_most_once`.
