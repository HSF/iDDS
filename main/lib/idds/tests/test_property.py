

class TestProperty:

    def __init__(self):
        # @self.IDDSProperty
        pass

    def IDDSProperty(attribute):
        def _get(self, attribute):
            self.get_metadata_item(attribute, None)

        def _set(self, attribute, value):
            self.add_metadata_item(attribute, value)

        attribute = property(_get, _set)
        return attribute
