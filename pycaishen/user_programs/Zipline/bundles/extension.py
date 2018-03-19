"""
to put in ./zipline/extension.py
"""

from zipline.data.bundles import register

from zipline.data.bundles.google import google_equities

equities2 = {
    'JSE:ADR',
}

register(
    'my-google-equities-bundle',  # name this whatever you like
    google_equities(equities2),
)