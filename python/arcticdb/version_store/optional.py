
def assume_true(name, kwargs):
    if name in kwargs and kwargs.get(name) is False:
        return False
    else:
        return True


def assume_false(name, kwargs):
    if name not in kwargs or kwargs.get(name) is False:
        return False
    else:
        return True

