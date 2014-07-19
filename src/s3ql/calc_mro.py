'''
calc_mro.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2011 Steven D'Aprano

This file may be distributed under the terms of the MIT License.

The original source code was retrieved from
http://code.activestate.com/recipes/577748-calculate-the-mro-of-a-class/
'''

def calc_mro(*bases):
    """Calculate the Method Resolution Order of bases using the C3 algorithm.

    Suppose you intended creating a class K with the given base classes. This
    function returns the MRO which K would have, *excluding* K itself (since
    it doesn't yet exist), as if you had actually created the class.

    Another way of looking at this, if you pass a single class K, this will
    return the linearization of K (the MRO of K, *including* itself).
    """
    seqs = [list(C.__mro__) for C in bases] + [list(bases)]
    res = []
    while True:
      non_empty = list(filter(None, seqs))
      if not non_empty:
          # Nothing left to process, we're done.
          return tuple(res)
      for seq in non_empty:  # Find merge candidates among seq heads.
          candidate = seq[0]
          not_head = [s for s in non_empty if candidate in s[1:]]
          if not_head:
              # Reject the candidate.
              candidate = None
          else:
              break
      if not candidate:
          raise TypeError("inconsistent hierarchy, no C3 MRO is possible")
      res.append(candidate)
      for seq in non_empty:
          # Remove candidate.
          if seq[0] == candidate:
              del seq[0]



if __name__ == '__main__':
    # Run self-tests. Prints nothing if they succeed.
    O = object
    class SeriousOrderDisagreement:
        class X(O): pass
        class Y(O): pass
        class A(X, Y): pass
        class B(Y, X): pass
        bases = (A, B)

    try:
        x = calc_mro(*SeriousOrderDisagreement.bases)
    except TypeError:
        pass
    else:
        print("failed test, mro should have raised but got %s instead" % (x,))

    class Example0:  # Trivial single inheritance case.
        class A(O): pass
        class B(A): pass
        class C(B): pass
        class D(C): pass
        tester = D
        expected = (D, C, B, A, O)

    class Example1:
        class F(O): pass
        class E(O): pass
        class D(O): pass
        class C(D, F): pass
        class B(D, E): pass
        class A(B, C): pass
        tester = A
        expected = (A, B, C, D, E, F, O)

    class Example2:
        class F(O): pass
        class E(O): pass
        class D(O): pass
        class C(D, F): pass
        class B(E, D): pass
        class A(B, C): pass
        tester = A
        expected = (A, B, E, C, D, F, O)

    class Example3:
        class A(O): pass
        class B(O): pass
        class C(O): pass
        class D(O): pass
        class E(O): pass
        class K1(A, B, C): pass
        class K2(D, B, E): pass
        class K3(D, A): pass
        class Z(K1, K2, K3): pass

        assert calc_mro(A) == (A, O)
        assert calc_mro(B) == (B, O)
        assert calc_mro(C) == (C, O)
        assert calc_mro(D) == (D, O)
        assert calc_mro(E) == (E, O)
        assert calc_mro(K1) == (K1, A, B, C, O)
        assert calc_mro(K2) == (K2, D, B, E, O)
        assert calc_mro(K3) == (K3, D, A, O)

        tester = Z
        expected = (Z, K1, K2, K3, D, A, B, C, E, O)

    for example in [Example0, Example1, Example2, Example3]:
        # First test that the expected result is the same as what Python
        # actually generates.
        assert example.expected == example.tester.__mro__
        # Now check the calculated MRO.
        assert calc_mro(example.tester) == example.expected
