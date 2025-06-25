//! The supported DMAP file formats.

/// The [FitACF file format](https://radar-software-toolkit-rst.readthedocs.io/en/latest/references/general/fitacf/)
pub mod fitacf;

/// The [Grid file format](https://radar-software-toolkit-rst.readthedocs.io/en/latest/references/general/grid/)
pub mod grid;

/// The [IQDat file format](https://radar-software-toolkit-rst.readthedocs.io/en/latest/references/general/iqdat/)
pub mod iqdat;

/// The [Map file format](https://radar-software-toolkit-rst.readthedocs.io/en/latest/references/general/map/)
pub mod map;

/// The [RawACF file format](https://radar-software-toolkit-rst.readthedocs.io/en/latest/references/general/rawacf/)
pub mod rawacf;

/// The [SND file format](https://github.com/SuperDARN/rst/pull/315)
pub mod snd;

/// The generic [Dmap file format](https://radar-software-toolkit-rst.readthedocs.io/en/latest/references/general/dmap_data/)
pub mod dmap;
