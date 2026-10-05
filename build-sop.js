const fs = require('fs');
const path = require('path');
const { Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, ImageRun,
        Header, Footer, AlignmentType, LevelFormat,
        TabStopType, TabStopPosition, HeadingLevel, BorderStyle, WidthType, ShadingType,
        PageNumber, PageBreak } = require('docx');

const SS = p => path.join(__dirname, 'sop-screenshots', p);
const NAVY = '172C45', MIDNIGHT = '020F18', TEAL = '72A4BF', MIDNAVY = '1D4053';
const FONT = 'Libre Baskerville';

// All screenshots are 1568x710. Content width 6.5in => 624px wide at 96dpi.
const IMG_W = 624, IMG_H = Math.round(624 * 710 / 1568);

function shot(file, alt) {
  return new Paragraph({
    spacing: { before: 120, after: 60 },
    children: [new ImageRun({
      type: 'jpg',
      data: fs.readFileSync(SS(file)),
      transformation: { width: IMG_W, height: IMG_H },
      altText: { title: alt, description: alt, name: alt },
    })],
  });
}

function caption(text) {
  return new Paragraph({
    spacing: { after: 240 },
    children: [new TextRun({ text, font: FONT, size: 18, italics: true, color: TEAL })],
  });
}

function stepLabel(num) {
  return new Paragraph({
    spacing: { before: 320, after: 40 },
    children: [new TextRun({ text: `STEP ${String(num).padStart(2, '0')}`, font: FONT, size: 18, bold: true, color: TEAL })],
  });
}

function h2(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(text)] });
}

function body(runs, opts = {}) {
  const children = (Array.isArray(runs) ? runs : [runs]).map(r =>
    typeof r === 'string' ? new TextRun({ text: r, font: FONT, size: 22, color: MIDNIGHT }) : r);
  return new Paragraph({ spacing: { after: 160, line: 320 }, ...opts, children });
}

function bold(text) { return new TextRun({ text, font: FONT, size: 22, bold: true, color: NAVY }); }

function bullet(runs) {
  return body(runs, { numbering: { reference: 'bullets', level: 0 } });
}

const cellBorder = { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' };
const borders = { top: cellBorder, bottom: cellBorder, left: cellBorder, right: cellBorder };

const COLS = [2300, 7060];
function row(cells, header = false) {
  return new TableRow({
    children: cells.map((text, i) => new TableCell({
      borders, width: { size: COLS[i], type: WidthType.DXA },
      shading: header ? { fill: NAVY, type: ShadingType.CLEAR } : undefined,
      margins: { top: 80, bottom: 80, left: 120, right: 120 },
      children: [new Paragraph({ children: [new TextRun({ text, font: FONT, size: 20, bold: header || i === 0, color: header ? 'FFFFFF' : MIDNIGHT })] })],
    })),
  });
}

function fieldsTable(rows) {
  return new Table({
    width: { size: 9360, type: WidthType.DXA },
    columnWidths: COLS,
    rows: [row(['Field', 'What to enter'], true), ...rows.map(r => row(r))],
  });
}

const doc = new Document({
  styles: {
    default: { document: { run: { font: FONT, size: 22, color: MIDNIGHT } } },
    paragraphStyles: [
      { id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 40, bold: true, font: FONT, color: NAVY },
        paragraph: { spacing: { before: 240, after: 240 }, outlineLevel: 0 } },
      { id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 28, bold: true, font: FONT, color: NAVY },
        paragraph: { spacing: { before: 120, after: 120 }, outlineLevel: 1 } },
    ],
  },
  numbering: {
    config: [
      { reference: 'bullets', levels: [{ level: 0, format: LevelFormat.BULLET, text: '•', alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
    ],
  },
  sections: [{
    properties: {
      page: { size: { width: 12240, height: 15840 }, margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 } },
    },
    headers: {
      default: new Header({ children: [new Paragraph({
        tabStops: [{ type: TabStopType.RIGHT, position: TabStopPosition.MAX }],
        border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: TEAL, space: 4 } },
        children: [
          new TextRun({ text: 'frontrow', font: FONT, size: 20, color: NAVY }),
          new TextRun({ text: 'MD', font: FONT, size: 20, bold: true, color: NAVY }),
          new TextRun({ text: '\tSOP — Publishing a Blog Post', font: FONT, size: 18, color: TEAL }),
        ],
      })] }),
    },
    footers: {
      default: new Footer({ children: [new Paragraph({
        tabStops: [{ type: TabStopType.RIGHT, position: TabStopPosition.MAX }],
        children: [
          new TextRun({ text: 'frontrowmd.com · Webflow', font: FONT, size: 18, color: TEAL }),
          new TextRun({ text: '\tPage ', font: FONT, size: 18, color: NAVY }),
          new TextRun({ children: [PageNumber.CURRENT], font: FONT, size: 18, color: NAVY }),
        ],
      })] }),
    },
    children: [
      // ===== Title =====
      new Paragraph({ heading: HeadingLevel.HEADING_1, spacing: { before: 0, after: 80 },
        children: [new TextRun('Publishing a New Blog Post')] }),
      new Paragraph({ spacing: { after: 240 }, children: [
        new TextRun({ text: 'Webflow → Frontrowmd site → CMS → Blogs  ·  Posts go live at www.frontrowmd.com/blog/', font: FONT, size: 20, color: TEAL }),
      ] }),

      // ===== Required fields up front =====
      h2('Required fields'),
      body('Every post needs all 8 of these before it can be created:'),
      fieldsTable([
        ['Name', 'Post title. The Slug (URL) auto-generates from it.'],
        ['Slug', 'Auto-fills from Name. Lowercase, hyphenated → www.frontrowmd.com/blog/<slug>'],
        ['Publish date', 'Date shown on the post (MM/DD/YYYY).'],
        ['Author', 'Pick from dropdown.'],
        ['Category', 'Pick one: Case Studies, Trust & Credibility, Clinician Validation, Conversion & ROI, Consumer Trends.'],
        ['Read time in minutes', 'A number, e.g. 3.'],
        ['Image', 'Header/thumbnail image — see sizes below.'],
        ['Article', 'Full post body (rich text: headings, images, embeds).'],
      ]),
      new Paragraph({ spacing: { before: 200 }, children: [] }),

      h2('Optional (recommended for SEO)'),
      fieldsTable([
        ['Sub heading', 'One supporting line shown under the title.'],
        ['Meta Title', 'Google result title — usually same as Name.'],
        ['Meta Description', '1–2 sentence search summary, ~150 characters.'],
      ]),
      new Paragraph({ spacing: { before: 200 }, children: [] }),

      h2('Image sizes'),
      bullet([bold('Header image: '), '2674 × 1308 px (≈2:1 wide). Match this — it is what existing posts use. Keep under ~500 KB; WebP or AVIF preferred.']),
      bullet([bold('Images inside the Article: '), '1600 px wide is plenty. Compress before uploading.']),

      new Paragraph({ children: [new PageBreak()] }),

      // ===== Steps =====
      stepLabel(1),
      h2('Open the site'),
      body(['Log in at webflow.com/dashboard and open the ', bold('Frontrowmd'), ' project (the "Business" card).']),
      shot('ss_72366s2yl.jpeg', 'Webflow dashboard with the Frontrowmd project'),
      caption('Click the "Frontrowmd" (Business) card.'),

      stepLabel(2),
      h2('Go to CMS → Blogs'),
      body(['In the Designer, click ', bold('CMS'), ' (top-left), then the ', bold('Blogs'), ' collection in the sidebar.']),
      shot('ss_8528bdvl5.jpeg', 'Blogs collection listing all posts'),
      caption('The Blogs collection — every post and its publish status.'),

      stepLabel(3),
      h2('Click + New Blog and fill in the fields'),
      body(['Click ', bold('+ New Blog'), ' (top-right). Fill in everything from the tables on page 1 — required fields are marked with a red asterisk in the form.']),
      shot('ss_9993yv9mu.jpeg', 'Empty New Blog form with Name and Slug fields'),
      caption('The new post form. Note the URL preview under Slug.'),
      shot('ss_2023889bw.jpeg', 'Custom fields section of the New Blog form'),
      caption('SEO fields, date, author, category, read time, image, and article body.'),

      new Paragraph({ children: [new PageBreak()] }),

      stepLabel(4),
      h2('Publish the post'),
      body(['Top-right, click the arrow next to ', bold('Create draft'), ' and pick:']),
      bullet([bold('Publish now'), ' — live immediately. ', new TextRun({ text: '(The usual choice.)', font: FONT, size: 22, italics: true, color: MIDNAVY })]),
      bullet([bold('Create draft'), ' — saved but not live (needs a site publish later).']),
      bullet([bold('Schedule to publish later'), ' — goes live at a set date/time.']),
      shot('ss_9234vi6zb.jpeg', 'Create draft button dropdown with publish options'),
      caption('Publish options next to the Create draft button.'),

      stepLabel(5),
      h2('Verify'),
      body(['Open ', bold('www.frontrowmd.com/blog'), ' — check the post appears in the listing, then open it and check images, headings, and embeds.']),
      body(['If it is not live: it was saved as a draft. Go to the ', bold('Design'), ' tab → ', bold('Publish'), ' (top-right) → check ', bold('www.frontrowmd.com'), ' → ', bold('Publish to selected domains'), '. Note this pushes ALL pending site changes, not just your post.']),
      shot('ss_8517iqm8g.jpeg', 'Publish panel with staging and production domains'),
      caption('Site publish panel — only needed for drafts/queued posts.'),
    ],
  }],
});

Packer.toBuffer(doc).then(buf => {
  const out = path.join(__dirname, 'FrontrowMD - Blog Publishing SOP (Webflow).docx');
  fs.writeFileSync(out, buf);
  console.log('written', out, buf.length);
});
