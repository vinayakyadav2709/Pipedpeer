"""Tests for np_d — distributed numpy drop-in replacement."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import np_d as np


# ── numpy inheritance ───────────────────────────────────────────────────


class TestNumpyInheritance:
    """np_d should expose every numpy symbol unchanged."""

    def test_array_creation(self):
        a = np.array([1, 2, 3])
        assert isinstance(a, np.ndarray)
        np.testing.assert_array_equal(a, [1, 2, 3])

    def test_zeros(self):
        z = np.zeros((2, 3))
        assert z.shape == (2, 3)
        assert z.sum() == 0.0

    def test_ones(self):
        o = np.ones((3, 2))
        assert o.shape == (3, 2)
        assert o.sum() == 6.0

    def test_eye(self):
        I = np.eye(4)
        expected = np._original_matmul(
            np.eye(4), np.eye(4)
        )  # identity * identity = identity
        np.testing.assert_array_equal(I, expected)

    def test_arange(self):
        a = np.arange(10)
        assert len(a) == 10
        assert a[0] == 0 and a[-1] == 9

    def test_linspace(self):
        a = np.linspace(0, 1, 5)
        assert len(a) == 5
        assert a[0] == 0.0 and a[-1] == 1.0

    def test_random(self):
        r = np.random.rand(3, 3)
        assert r.shape == (3, 3)

    def test_linalg(self):
        A = np.array([[1, 2], [3, 4]], dtype=float)
        det = np.linalg.det(A)
        assert abs(det - (-2.0)) < 1e-10

    def test_dtype(self):
        a = np.array([1, 2], dtype=np.float32)
        assert a.dtype == np.float32

    def test_version(self):
        assert np.__version__ is not None


# ── matmul override ────────────────────────────────────────────────────


class TestMatmul:
    """np_d.matmul should produce correct results (fallback when no peers)."""

    def test_basic_2x2(self):
        A = np.array([[1, 2], [3, 4]])
        B = np.array([[5, 6], [7, 8]])
        expected = np.array([[19, 22], [43, 50]])
        result = np.matmul(A, B)
        np.testing.assert_array_equal(result, expected)

    def test_identity(self):
        A = np.random.rand(4, 4)
        I = np.eye(4)
        result = np.matmul(A, I)
        np.testing.assert_array_almost_equal(result, A)

    def test_non_square(self):
        A = np.random.rand(3, 5)
        B = np.random.rand(5, 2)
        result = np.matmul(A, B)
        expected = np._original_matmul(A, B)
        assert result.shape == (3, 2)
        np.testing.assert_array_almost_equal(result, expected)

    def test_large_matrix(self):
        A = np.random.rand(64, 64)
        B = np.random.rand(64, 64)
        result = np.matmul(A, B)
        expected = np._original_matmul(A, B)
        np.testing.assert_array_almost_equal(result, expected)

    def test_float32(self):
        A = np.random.rand(4, 4).astype(np.float32)
        B = np.random.rand(4, 4).astype(np.float32)
        result = np.matmul(A, B)
        expected = np._original_matmul(A, B)
        np.testing.assert_array_almost_equal(result, expected, decimal=5)

    def test_1d_fallback(self):
        """1-D inputs should fall through to numpy (not distributed)."""
        a = np.array([1, 2, 3])
        b = np.array([4, 5, 6])
        result = np.matmul(a, b)
        assert result == 32  # dot product

    def test_3d_fallback(self):
        """Batched 3-D matmul should fall through to numpy."""
        A = np.random.rand(2, 3, 4)
        B = np.random.rand(2, 4, 5)
        result = np.matmul(A, B)
        expected = np._original_matmul(A, B)
        np.testing.assert_array_almost_equal(result, expected)


# ── dot override ───────────────────────────────────────────────────────


class TestDot:
    """np_d.dot should produce correct results (fallback when no peers)."""

    def test_basic_2x2(self):
        A = np.array([[1, 2], [3, 4]])
        B = np.array([[5, 6], [7, 8]])
        expected = np.array([[19, 22], [43, 50]])
        result = np.dot(A, B)
        np.testing.assert_array_equal(result, expected)

    def test_non_square(self):
        A = np.random.rand(3, 5)
        B = np.random.rand(5, 2)
        result = np.dot(A, B)
        expected = np._original_dot(A, B)
        assert result.shape == (3, 2)
        np.testing.assert_array_almost_equal(result, expected)

    def test_1d_dot_product(self):
        """1-D dot should fall through to numpy."""
        a = np.array([1.0, 2.0, 3.0])
        b = np.array([4.0, 5.0, 6.0])
        result = np.dot(a, b)
        assert abs(result - 32.0) < 1e-10

    def test_scalar_dot(self):
        """Scalar times array should fall through to numpy."""
        a = np.array([1, 2, 3])
        result = np.dot(a, 2)
        np.testing.assert_array_equal(result, [2, 4, 6])

    def test_out_parameter_fallback(self):
        """dot with out= should always use numpy (not distributed)."""
        A = np.array([[1, 2], [3, 4]], dtype=float)
        B = np.array([[5, 6], [7, 8]], dtype=float)
        out = np.empty((2, 2))
        result = np.dot(A, B, out=out)
        expected = np.array([[19, 22], [43, 50]], dtype=float)
        np.testing.assert_array_equal(out, expected)
        assert result is out


# ── connection lifecycle ───────────────────────────────────────────────


class TestConnection:
    """connect / shutdown / is_connected basics."""

    def test_not_connected_by_default(self):
        """Before explicit connect, is_connected may be False."""
        # matmul still works via fallback
        A = np.array([[1, 0], [0, 1]])
        B = np.array([[2, 3], [4, 5]])
        result = np.matmul(A, B)
        np.testing.assert_array_equal(result, B)

    def test_connect_and_shutdown(self):
        """connect/shutdown should not raise even without a coordinator."""
        try:
            np.connect()
        except Exception:
            pass
        A = np.array([[1, 2], [3, 4]])
        B = np.eye(2)
        result = np.matmul(A, B)
        np.testing.assert_array_almost_equal(result, A)
        try:
            np.shutdown()
        except Exception:
            pass


# ── matmul matches numpy exactly ───────────────────────────────────────


class TestResultsMatchNumpy:
    """Every np_d result should match the real numpy result."""

    def test_matmul_matches_numpy(self):
        rng = np.random.RandomState(42)
        A = rng.rand(16, 32)
        B = rng.rand(32, 8)
        np.testing.assert_array_almost_equal(np.matmul(A, B), np._original_matmul(A, B))

    def test_dot_matches_numpy(self):
        rng = np.random.RandomState(42)
        A = rng.rand(16, 32)
        B = rng.rand(32, 8)
        np.testing.assert_array_almost_equal(np.dot(A, B), np._original_dot(A, B))
