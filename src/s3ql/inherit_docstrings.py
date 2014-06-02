'''
inherit_docstrings.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.

---

This module defines a metaclass and function decorator that allows
to inherit the docstring for a function from the superclass.
'''


from functools import partial
from abc import ABCMeta
from .calc_mro import calc_mro

__all__ = [ 'copy_ancestor_docstring', 'prepend_ancestor_docstring',
            'InheritableDocstrings', 'ABCDocstMeta' ]

# This definition is only used to assist static code analyzers
def copy_ancestor_docstring(fn):
    '''Copy docstring for method from superclass

    For this decorator to work, the class has to use the `InheritableDocstrings`
    metaclass.
    '''
    raise RuntimeError('Decorator can only be used in classes '
                       'using the `InheritableDocstrings` metaclass')

def _copy_ancestor_docstring(mro, fn):
    '''Decorator to set docstring for *fn* from *mro*'''

    if fn.__doc__ is not None:
        raise RuntimeError('Function already has docstring')

    # Search for docstring in superclass
    for cls in mro:
        super_fn = getattr(cls, fn.__name__, None)
        if super_fn is None:
            continue
        fn.__doc__ = super_fn.__doc__
        break
    else:
        raise RuntimeError("Can't inherit docstring for %s: method does not "
                           "exist in superclass" % fn.__name__)

    return fn

# This definition is only used to assist static code analyzers
def prepend_ancestor_docstring(fn):
    '''Prepend docstring from superclass method

    For this decorator to work, the class has to use the `InheritableDocstrings`
    metaclass.
    '''
    raise RuntimeError('Decorator can only be used in classes '
                       'using the `InheritableDocstrings` metaclass')

def _prepend_ancestor_docstring(mro, fn):
    '''Decorator to prepend ancestor docstring to *fn*'''

    if fn.__doc__ is None:
        fn.__doc__ = ''

    # Search for docstring in superclass
    for cls in mro:
        super_fn = getattr(cls, fn.__name__, None)
        if super_fn is None:
            continue
        if super_fn.__doc__.endswith('\n') and fn.__doc__.startswith('\n'):
            fn.__doc__ = super_fn.__doc__ + fn.__doc__
        else:
            fn.__doc__ = '%s\n%s' % (super_fn.__doc__, fn.__doc__)
        break
    else:
        raise RuntimeError("Can't find ancestor docstring for %s: method does not "
                           "exist in superclass" % fn.__name__)

    return fn

DECORATORS = (('copy_ancestor_docstring', _copy_ancestor_docstring),
              ('prepend_ancestor_docstring', _prepend_ancestor_docstring))

class InheritableDocstrings(type):

    @classmethod
    def __prepare__(cls, name, bases, **kwds):
        classdict = super().__prepare__(name, bases, *kwds)
        mro = calc_mro(*bases)

        # Inject decorators into class namespace
        for (name, fn) in DECORATORS:
            classdict[name] = partial(fn, mro)

        return classdict

    def __new__(cls, name, bases, classdict):
        for (dec_name, fn) in DECORATORS:
            # Decorators may not exist in class dict if the class (metaclass
            # instance) was constructed with an explicit call to `type`
            # (Pythonbug? reported as http://bugs.python.org/issue18334)
            if dec_name not in classdict:
                continue

            # Make sure that class definition hasn't messed with decorator
            if getattr(classdict[dec_name], 'func', None) is not fn:
                raise RuntimeError('No %s attribute may be created in classes using '
                                   'the InheritableDocstrings metaclass' % name)


            # Delete decorator from class namespace
            del classdict[dec_name]

        return super().__new__(cls, name, bases, classdict)

# Derive new metaclass to add docstring inheritance
class ABCDocstMeta(ABCMeta, InheritableDocstrings):
    pass
