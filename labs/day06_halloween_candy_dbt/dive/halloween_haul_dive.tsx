import { useSQLQuery, useDiveState } from '@motherduck/react-sql-query';
import {
	Bar,
	BarChart,
	CartesianGrid,
	LabelList,
	Line,
	LineChart,
	ResponsiveContainer,
	Scatter,
	ScatterChart,
	Tooltip,
	XAxis,
	YAxis,
	ZAxis,
} from 'recharts';

// Saved to MotherDuck as the "Ari's Halloween Haul 2026" Dive.
// Reads the marts built by this dbt project (`make build-md`).
export const REQUIRED_DATABASES = [
	{ type: 'database', path: 'md:halloween_candy', alias: 'halloween_candy' },
];

const N = (value: unknown): number => (value == null ? 0 : Number(value));

const COLORS = {
	primary: '#0777b3',
	secondary: '#e18727',
	muted: '#adadad',
	bg: '#f8f8f8',
	text: '#231f20',
	mutedText: '#6a6a6a',
};

const CATEGORY_LABEL: Record<string, string> = {
	chocolate: 'Chocolate',
	fruity: 'Fruity',
	gummy: 'Gummy',
	sour: 'Sour',
	hard_candy: 'Hard candy',
	non_candy: 'Not candy',
	snack: 'Snack',
	gum: 'Gum',
};

type Tab = 'haul' | 'breakdown' | 'trading';
const TABS: { key: Tab; label: string }[] = [
	{ key: 'haul', label: 'The haul' },
	{ key: 'breakdown', label: 'Breakdown' },
	{ key: 'trading', label: 'Trading' },
];

function rowsOf(query: { data: unknown }): any[] {
	return Array.isArray(query.data) ? (query.data as any[]) : [];
}

function ChartSkeleton({ height = 240 }: { height?: number }) {
	return <div className="animate-pulse rounded bg-gray-100" style={{ height }} />;
}

function QueryError({ error }: { error: unknown }) {
	return (
		<p role="alert" className="text-sm" style={{ color: '#bc1200' }}>
			{String(error)}
		</p>
	);
}

function SectionTitle({ title, subtitle }: { title: string; subtitle?: string }) {
	return (
		<div className="mb-2">
			<h2 className="text-sm font-semibold" style={{ color: COLORS.text }}>
				{title}
			</h2>
			{subtitle && (
				<p className="text-xs" style={{ color: COLORS.mutedText }}>
					{subtitle}
				</p>
			)}
		</div>
	);
}

// ── KPIs ──────────────────────────────────────────────────────────────

function Kpis() {
	const q = useSQLQuery(`
    SELECT total_pieces, candy_types, pct_chocolate, parent_tax_pieces,
           est_total_sugar_kg, most_common_candy, non_candy_pieces
    FROM "halloween_candy"."marts"."mart_haul_summary"
  `);
	const r = rowsOf(q)[0];

	const kpis = [
		{ label: 'Total pieces', value: r ? N(r.total_pieces).toLocaleString() : '' },
		{ label: 'Chocolate share', value: r ? `${N(r.pct_chocolate).toFixed(1)}%` : '' },
		{ label: 'Est. sugar', value: r ? `${N(r.est_total_sugar_kg).toFixed(2)} kg` : '' },
		{ label: 'Most common', value: r ? String(r.most_common_candy) : '' },
	];

	return (
		<div className="mb-6">
			{q.isError ? (
				<QueryError error={q.error} />
			) : (
				<div className="grid grid-cols-4 gap-8">
					{kpis.map((k) => (
						<div key={k.label}>
							{q.isLoading ? (
								<div className="h-12 w-24 animate-pulse rounded bg-gray-200" />
							) : (
								<p className="text-4xl font-bold" style={{ color: COLORS.text }}>
									{k.value}
								</p>
							)}
							<p className="mt-1 text-sm" style={{ color: COLORS.mutedText }}>
								{k.label}
							</p>
						</div>
					))}
				</div>
			)}
			{r && (
				<p className="mt-3 text-xs" style={{ color: COLORS.mutedText }}>
					{N(r.candy_types)} kinds · {N(r.non_candy_pieces)} non-candy items (chips,
					Play-Doh, granola) · {N(r.parent_tax_pieces)} chocolate pieces taken as
					parent tax
				</p>
			)}
		</div>
	);
}

// ── Tab: The haul ─────────────────────────────────────────────────────

function TopCandies() {
	const q = useSQLQuery(`
    SELECT display_name, piece_count
    FROM "halloween_candy"."marts"."fct_candy_haul"
    ORDER BY piece_count DESC, display_name
    LIMIT 10
  `);
	const data = rowsOf(q).map((r) => ({
		name: String(r.display_name),
		pieces: N(r.piece_count),
	}));

	return (
		<section>
			<SectionTitle title="Top 10 by pieces" subtitle="All items, including non-candy" />
			{q.isLoading ? (
				<ChartSkeleton height={260} />
			) : q.isError ? (
				<QueryError error={q.error} />
			) : (
				<ResponsiveContainer width="100%" height={260}>
					<BarChart data={data} layout="vertical" margin={{ left: 8, right: 24 }}>
						<CartesianGrid strokeDasharray="3 3" stroke="#eee" horizontal={false} />
						<XAxis type="number" fontSize={11} allowDecimals={false} />
						<YAxis type="category" dataKey="name" width={160} fontSize={11} interval={0} />
						<Tooltip formatter={(v: number) => [`${v} pieces`, 'Count']} />
						<Bar dataKey="pieces" fill={COLORS.primary} barSize={14}>
							<LabelList dataKey="pieces" position="right" fontSize={11} fill={COLORS.mutedText} />
						</Bar>
					</BarChart>
				</ResponsiveContainer>
			)}
		</section>
	);
}

function PiecesLeft() {
	const q = useSQLQuery(`
    WITH r AS (SELECT * FROM "halloween_candy"."marts"."mart_candy_rationing"),
    total AS (SELECT count(*) AS n FROM r),
    spine AS (SELECT unnest(range(0, (SELECT max(day_number) FROM r) + 1)) AS day_number),
    daily AS (SELECT day_number, count(*) AS eaten FROM r GROUP BY 1)
    SELECT
      spine.day_number,
      strftime(DATE '2026-10-31' + to_days(spine.day_number::INT), '%b %d') AS day_label,
      (SELECT n FROM total) - COALESCE(sum(daily.eaten) OVER (ORDER BY spine.day_number), 0) AS pieces_left
    FROM spine LEFT JOIN daily USING (day_number)
    ORDER BY spine.day_number
  `);
	const rows = rowsOf(q);
	const data = rows.map((r) => ({
		day: String(r.day_label),
		left: N(r.pieces_left),
	}));
	const start = data[0]?.left ?? 0;
	const lastDay = data[data.length - 1]?.day ?? '';

	return (
		<section>
			<SectionTitle
				title="Pieces left over time"
				subtitle={
					data.length
						? `${start} candy pieces after parent tax, 3 per day, best first. Last piece: ${lastDay}`
						: 'Candy after parent tax, 3 pieces per day'
				}
			/>
			{q.isLoading ? (
				<ChartSkeleton height={260} />
			) : q.isError ? (
				<QueryError error={q.error} />
			) : (
				<ResponsiveContainer width="100%" height={260}>
					<LineChart data={data} margin={{ right: 16 }}>
						<CartesianGrid strokeDasharray="3 3" stroke="#eee" />
						<XAxis dataKey="day" fontSize={11} interval={6} />
						<YAxis fontSize={11} allowDecimals={false} />
						<Tooltip formatter={(v: number) => [`${v} pieces`, 'Left']} />
						<Line type="linear" dataKey="left" stroke={COLORS.primary} strokeWidth={2} dot={false} />
					</LineChart>
				</ResponsiveContainer>
			)}
		</section>
	);
}

// ── Tab: Breakdown ────────────────────────────────────────────────────

function Categories() {
	const q = useSQLQuery(`
    SELECT category, pieces, pct_of_haul
    FROM "halloween_candy"."marts"."mart_category_summary"
    ORDER BY pieces DESC
  `);
	const data = rowsOf(q).map((r) => ({
		category: CATEGORY_LABEL[String(r.category)] ?? String(r.category),
		pieces: N(r.pieces),
		pct: N(r.pct_of_haul),
	}));

	return (
		<section>
			<SectionTitle title="Pieces by category" />
			{q.isLoading ? (
				<ChartSkeleton height={240} />
			) : q.isError ? (
				<QueryError error={q.error} />
			) : (
				<ResponsiveContainer width="100%" height={240}>
					<BarChart data={data} layout="vertical" margin={{ left: 8, right: 40 }}>
						<CartesianGrid strokeDasharray="3 3" stroke="#eee" horizontal={false} />
						<XAxis type="number" fontSize={11} allowDecimals={false} />
						<YAxis type="category" dataKey="category" width={90} fontSize={11} interval={0} />
						<Tooltip
							formatter={(v: number, _n: string, item: any) => [
								`${v} pieces (${item.payload.pct.toFixed(1)}%)`,
								'Count',
							]}
						/>
						<Bar dataKey="pieces" fill={COLORS.primary} barSize={14}>
							<LabelList
								dataKey="pct"
								position="right"
								fontSize={11}
								fill={COLORS.mutedText}
								formatter={(v: number) => `${v.toFixed(0)}%`}
							/>
						</Bar>
					</BarChart>
				</ResponsiveContainer>
			)}
		</section>
	);
}

function Manufacturers() {
	const q = useSQLQuery(`
    SELECT manufacturer, pieces, market_share_pct, candy_types
    FROM "halloween_candy"."marts"."mart_manufacturer_summary"
    ORDER BY pieces DESC
    LIMIT 6
  `);
	const rows = rowsOf(q);

	return (
		<section>
			<SectionTitle title="Market share by maker" subtitle="Top 6 of 13 makers" />
			{q.isLoading ? (
				<ChartSkeleton height={180} />
			) : q.isError ? (
				<QueryError error={q.error} />
			) : (
				<table className="w-full text-sm" style={{ color: COLORS.text }}>
					<thead>
						<tr className="text-left text-xs" style={{ color: COLORS.mutedText }}>
							<th className="py-1 font-normal">Maker</th>
							<th className="py-1 text-right font-normal">Kinds</th>
							<th className="py-1 text-right font-normal">Pieces</th>
							<th className="py-1 text-right font-normal">Share</th>
						</tr>
					</thead>
					<tbody>
						{rows.map((r) => (
							<tr key={String(r.manufacturer)} style={{ borderTop: '1px solid #eee' }}>
								<td className="py-1">{String(r.manufacturer)}</td>
								<td className="py-1 text-right">{N(r.candy_types)}</td>
								<td className="py-1 text-right">{N(r.pieces)}</td>
								<td className="py-1 text-right">{N(r.market_share_pct).toFixed(1)}%</td>
							</tr>
						))}
					</tbody>
				</table>
			)}
		</section>
	);
}

function Allergens() {
	const q = useSQLQuery(`
    SELECT allergen_group, pieces, array_to_string(candies, ', ') AS candies
    FROM "halloween_candy"."marts"."mart_allergen_report"
    ORDER BY pieces DESC
  `);
	const rows = rowsOf(q);

	return (
		<section>
			<SectionTitle
				title="Nut allergens"
				subtitle="From a hand-made lookup, not labels. Check wrappers before sharing."
			/>
			{q.isLoading ? (
				<ChartSkeleton height={120} />
			) : q.isError ? (
				<QueryError error={q.error} />
			) : (
				<table className="w-full text-sm" style={{ color: COLORS.text }}>
					<tbody>
						{rows.map((r) => {
							const group = String(r.allergen_group);
							const nutFree = group.startsWith('nut-free');
							return (
								<tr key={group} style={{ borderTop: '1px solid #eee' }}>
									<td className="py-1 pr-3 align-top whitespace-nowrap font-medium">
										{nutFree ? 'No nuts listed' : `Contains ${group}`}
									</td>
									<td className="py-1 pr-3 align-top text-right">{N(r.pieces)}</td>
									<td className="py-1 align-top text-xs" style={{ color: COLORS.mutedText }}>
										{nutFree ? 'Everything else' : String(r.candies)}
									</td>
								</tr>
							);
						})}
					</tbody>
				</table>
			)}
		</section>
	);
}

// ── Tab: Trading ──────────────────────────────────────────────────────

// "Reese's Peanut Butter Cup" -> "Reese's" so labels stay on one line.
const shortName = (name: string) => (name.length > 12 ? name.split(' ')[0] : name);

type Point = { pieces: number; value: number; names: string[]; n: number };

function TradeScatter() {
	const q = useSQLQuery(`
    SELECT display_name, piece_count, trade_value
    FROM "halloween_candy"."marts"."fct_candy_haul"
    WHERE is_candy
    ORDER BY trade_value DESC, piece_count DESC
  `);

	// Several candies share the same (pieces, value) spot: merge them into one bigger dot.
	const byXY = new Map<string, Point>();
	for (const r of rowsOf(q)) {
		const pieces = N(r.piece_count);
		const value = N(r.trade_value);
		const key = `${pieces}|${value}`;
		const pt = byXY.get(key) ?? { pieces, value, names: [], n: 0 };
		pt.names.push(String(r.display_name));
		pt.n += 1;
		byXY.set(key, pt);
	}
	const points = [...byXY.values()];
	// Label single candies with 6+ pieces; the rest are in the tooltip.
	const isLabeled = (p: Point) => p.n === 1 && p.pieces >= 6;
	const labeled = points.filter(isLabeled);
	const rest = points.filter((p) => !isLabeled(p));

	const renderLabel = (props: any) => {
		const { x, y, width, height, index } = props;
		const p = labeled[index];
		if (!p) return null;
		// If another labeled dot sits just left on the same row, put this label below.
		const below = labeled.some(
			(o) => o !== p && o.value === p.value && o.pieces < p.pieces && p.pieces - o.pieces <= 2,
		);
		const cx = N(x) + N(width) / 2;
		const cy = N(y) + N(height) / 2;
		return (
			<text
				x={cx}
				y={below ? cy + 16 : cy - 9}
				textAnchor="middle"
				fontSize={10}
				fill={COLORS.text}
			>
				{shortName(p.names[0])}
			</text>
		);
	};

	return (
		<section>
			<SectionTitle
				title="Trade value vs. how many Ari got"
				subtitle="Top right: lots of the good stuff. Bottom right: trade bait. Trade value 1–5 is a guess."
			/>
			{q.isLoading ? (
				<ChartSkeleton height={280} />
			) : q.isError ? (
				<QueryError error={q.error} />
			) : (
				<ResponsiveContainer width="100%" height={280}>
					<ScatterChart margin={{ top: 16, right: 24, bottom: 8, left: 0 }}>
						<CartesianGrid strokeDasharray="3 3" stroke="#eee" />
						<XAxis
							type="number"
							dataKey="pieces"
							name="Pieces"
							domain={[0, 16]}
							fontSize={11}
							allowDecimals={false}
							label={{ value: 'Pieces', position: 'insideBottomRight', offset: -4, fontSize: 11 }}
						/>
						<YAxis
							type="number"
							dataKey="value"
							name="Trade value"
							domain={[0.5, 5.5]}
							ticks={[1, 2, 3, 4, 5]}
							fontSize={11}
							width={32}
						/>
						<ZAxis type="number" dataKey="n" range={[50, 160]} domain={[1, 3]} />
						<Tooltip
							cursor={{ strokeDasharray: '3 3' }}
							content={({ payload }: any) => {
								const p: Point | undefined = payload?.[0]?.payload;
								if (!p) return null;
								return (
									<div className="rounded bg-white px-2 py-1 text-xs shadow" style={{ color: COLORS.text }}>
										<strong>{p.names.join(', ')}</strong>
										<br />
										{p.pieces} pieces each · trade value {p.value}
									</div>
								);
							}}
						/>
						<Scatter data={rest} fill={COLORS.primary} fillOpacity={0.55} />
						<Scatter data={labeled} fill={COLORS.primary}>
							<LabelList dataKey="pieces" content={renderLabel} />
						</Scatter>
					</ScatterChart>
				</ResponsiveContainer>
			)}
		</section>
	);
}

function TradeBait() {
	const q = useSQLQuery(`
    SELECT display_name, piece_count, trade_value
    FROM "halloween_candy"."marts"."fct_candy_haul"
    WHERE is_candy AND trade_value <= 2
    ORDER BY piece_count DESC, display_name
    LIMIT 6
  `);
	const rows = rowsOf(q);

	return (
		<section>
			<SectionTitle title="Trade these first" subtitle="Value 1–2, biggest piles first" />
			{q.isLoading ? (
				<ChartSkeleton height={160} />
			) : q.isError ? (
				<QueryError error={q.error} />
			) : (
				<table className="w-full text-sm" style={{ color: COLORS.text }}>
					<thead>
						<tr className="text-left text-xs" style={{ color: COLORS.mutedText }}>
							<th className="py-1 font-normal">Candy</th>
							<th className="py-1 text-right font-normal">Pcs</th>
							<th className="py-1 pl-2 text-right font-normal">Value</th>
						</tr>
					</thead>
					<tbody>
						{rows.map((r) => (
							<tr key={String(r.display_name)} style={{ borderTop: '1px solid #eee' }}>
								<td className="py-1">{String(r.display_name)}</td>
								<td className="py-1 text-right">{N(r.piece_count)}</td>
								<td className="py-1 pl-2 text-right" style={{ color: COLORS.mutedText }}>
									{N(r.trade_value)}
								</td>
							</tr>
						))}
					</tbody>
				</table>
			)}
		</section>
	);
}

// ── Page ──────────────────────────────────────────────────────────────

export default function HalloweenHaulDive() {
	const [tab, setTab] = useDiveState<Tab>('tab', 'haul');

	return (
		<main className="p-6" style={{ background: COLORS.bg, color: COLORS.text, minHeight: '100%' }}>
			<h1 className="text-2xl font-semibold">Ari's Halloween Haul 2026</h1>
			<p className="mb-6 text-sm" style={{ color: COLORS.mutedText }}>
				Trick-or-treating, Oct 31 2026 · Sugar, calorie and trade values are estimates
			</p>

			<Kpis />

			<div className="mb-4 flex gap-2" role="tablist">
				{TABS.map((t) => {
					const active = tab === t.key;
					return (
						<button
							key={t.key}
							role="tab"
							aria-selected={active}
							onClick={() => setTab(t.key)}
							className="rounded px-3 py-1 text-sm"
							style={{
								cursor: 'pointer',
								border: `1px solid ${active ? COLORS.primary : '#ddd'}`,
								background: active ? COLORS.primary : '#fff',
								color: active ? '#fff' : COLORS.text,
							}}
						>
							{t.label}
						</button>
					);
				})}
			</div>

			{tab === 'haul' && (
				<div className="grid grid-cols-2 gap-8">
					<TopCandies />
					<PiecesLeft />
				</div>
			)}

			{tab === 'breakdown' && (
				<div className="grid grid-cols-2 gap-8">
					<Categories />
					<div className="flex flex-col gap-6">
						<Manufacturers />
						<Allergens />
					</div>
				</div>
			)}

			{tab === 'trading' && (
				<div className="grid grid-cols-3 gap-8">
					<div style={{ gridColumn: 'span 2 / span 2' }}>
						<TradeScatter />
					</div>
					<TradeBait />
				</div>
			)}
		</main>
	);
}
