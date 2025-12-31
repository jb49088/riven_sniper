# riven_sniper

An automated [Warframe](https://www.warframe.com/) [riven mod](https://warframe.fandom.com/wiki/Riven_Mods) deal finder. Continuously scrapes [riven.market](https://riven.market/) and [warframe.market](https://warframe.market/), identifies top-tier stats combinations (godrolls) from historical pricing data, and sends instant [Pushover](https://pushover.net/) alerts when underpriced rivens appear. Runs on a [Raspberry Pi](https://www.raspberrypi.com/) with 10-second polling to catch good deals before they're gone.

<!-- CODE_STATISTICS_START -->

### Code Statistics

```
-------------------------------------------------------------------------------
Language                     files          blank        comment           code
-------------------------------------------------------------------------------
Python                           7            209            142            741
Markdown                         1              8              4             30
-------------------------------------------------------------------------------
SUM:                             8            217            146            771
-------------------------------------------------------------------------------
```
<!-- CODE_STATISTICS_END -->

<!-- PROJECT_STRUCTURE_START -->

### Project Structure

```
riven_sniper
├── data
├── logs
├── README.md
└── src
    ├── aggregator.py
    ├── config.py
    ├── monitor.py
    ├── normalizer.py
    ├── poller.py
    ├── riven_sniper.py
    └── scraper.py

4 directories, 8 files
```
<!-- PROJECT_STRUCTURE_END -->
