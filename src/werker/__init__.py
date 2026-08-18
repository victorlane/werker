__version__ = "0.1.0.dev0"

# No eager imports here: this module loads before the app registry is
# ready. Import the public API from its submodules instead, e.g.
# `from werker.decorators import at_most_once`.
