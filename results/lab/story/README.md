# The lab campaign as a story page

`index.html` is one self-contained file (data, d3 v7.9.0, styles and scripts inlined) that
opens offline from `file://`: a cluster replay of PipedPeer, Ray and Dask, then one chapter
per question (scaling, lines changed, elastic join, churn, curl to cluster, network delay,
where PipedPeer loses, inside one run). Dark first; the button at the top right switches to light.

- `screens/`: one screenshot per section, dark and light, from headless Chromium.
- `export/`: every chart as SVG, PNG (3x) and PDF, light (paper) and dark (slides);
  `hero-dark.png` and `hero-light.png` at 1920x1080; `hero.mp4` (1080p, 30 fps) and `hero.gif`.

Regenerate (needs python3, node 22+, chromium and ffmpeg):

    scripts/campaign/story.py --sample --export    # this sample + the headline rehearsal
    scripts/campaign/story.py --export             # the lab day: results/lab/runs.jsonl, otel/

While the data is the sample, every number is stamped "emulated, container lab, sample
sizes; not a result". A section without data draws a "lab day fills this" placeholder.
