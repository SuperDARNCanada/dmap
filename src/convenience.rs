/// Useful functions for reducing code duplication.

/// Splits a `Vec<Result<T, E>>` into three parts: `Vec<T>`, `Vec<E>`, and the indices
/// of the `E` results in the original `Vec`.
pub(crate) fn split_results<T, E>(dmap_results: Vec<Result<T, E>>) -> (Vec<T>, Vec<E>, Vec<usize>) {
    let mut ok_inners: Vec<T> = vec![];
    let mut bad_indices: Vec<usize> = vec![];
    let mut error_inners: Vec<E> = vec![];
    for (i, rec) in dmap_results.into_iter().enumerate() {
        match rec {
            Ok(x) => ok_inners.push(x),
            Err(e) => {
                error_inners.push(e);
                bad_indices.push(i);
            }
        }
    }

    (ok_inners, error_inners, bad_indices)
}


