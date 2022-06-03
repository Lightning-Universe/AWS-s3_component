r"""
To test a lightning component:

1. Init the component.
2. call .run()
"""
from lai_s3.component import TemplateComponent


def test_placeholder_component():
    messenger = TemplateComponent()
    messenger.run()
    assert messenger.value == 1
