import { useSQLQuery, useDiveState } from '@motherduck/react-sql-query';

// Saved to MotherDuck as the "Ari's Spooky Candy Treasure" Dive.
// Reads the marts built by this dbt project (`make build-md`).
// Built for a (nearly) four-year-old: big pictures, things to tap, counting games.
export const REQUIRED_DATABASES = [
	{ type: 'database', path: 'md:halloween_candy', alias: 'halloween_candy' },
];

const N = (value: unknown): number => (value == null ? 0 : Number(value));

function rowsOf(query: { data: unknown }): any[] {
	return Array.isArray(query.data) ? (query.data as any[]) : [];
}

// ── Look & feel ───────────────────────────────────────────────────────

const C = {
	night: '#1b0a33',
	night2: '#3b1466',
	orange: '#ff7a00',
	pumpkin: '#ff9a1f',
	purple: '#9b5de5',
	slime: '#7cff4f',
	moon: '#fff4c2',
	white: '#fffaf0',
	dim: '#cbb8ea',
};

const FONT =
	'"Comic Sans MS", "Chalkboard SE", "Marker Felt", "Comic Neue", ui-rounded, system-ui, sans-serif';

// One picture + wrapper color per candy (keyed by the raw name from the haul sheet).
const CANDY: Record<string, { e: string; bg: string }> = {
	KitKat: { e: '🍫', bg: '#d7262e' },
	Starbursts: { e: '⭐', bg: '#f5c400' },
	"Hershey's": { e: '🍫', bg: '#5a2d14' },
	Haribo: { e: '🐻', bg: '#e8b100' },
	Chips: { e: '🥔', bg: '#c98a2a' },
	"Reese's": { e: '🧁', bg: '#ff7a00' },
	Twizzlers: { e: '🍒', bg: '#c4122f' },
	Suckers: { e: '🍭', bg: '#e04fbf' },
	'Tootsie Rolls': { e: '🪵', bg: '#6b3a1e' },
	Twix: { e: '🥢', bg: '#c99a2e' },
	Warheads: { e: '😖', bg: '#27a844' },
	Skittles: { e: '🌈', bg: '#b0127a' },
	"M&M's Peanut": { e: '🥜', bg: '#f2c200' },
	'Jolly Ranchers Gu': { e: '🍏', bg: '#2a9d4b' },
	'Hard Candies': { e: '🍬', bg: '#2f80ed' },
	Snickers: { e: '😆', bg: '#7a4a24' },
	'3 Musketeers': { e: '⚔️', bg: '#3c6fb6' },
	"M&M's Chocolate": { e: '🔴', bg: '#6b3a1e' },
	'Almond Joy': { e: '🥥', bg: '#1f5fa8' },
	'Sour Patch Kids': { e: '🧒', bg: '#e0457b' },
	'Milky Way': { e: '🌌', bg: '#1e7f4f' },
	Airheads: { e: '☁️', bg: '#2fa4e7' },
	Playdoh: { e: '🎨', bg: '#f2c200' },
	Bubble: { e: '🫧', bg: '#ff8ac6' },
	Kinder: { e: '🥚', bg: '#e86a10' },
	Butterfinger: { e: '🧈', bg: '#2d5bd1' },
	'Rice Crispy': { e: '🍪', bg: '#2b7de0' },
	'Nerds Gummy': { e: '🤓', bg: '#8e44ad' },
	Sweettarts: { e: '💗', bg: '#d6337a' },
	Smarties: { e: '🎓', bg: '#7d5fff' },
	'Milk Duds': { e: '🥛', bg: '#c9a400' },
	'Granola Bars': { e: '🥣', bg: '#8a6d3b' },
	'Life Saver Gummie': { e: '🛟', bg: '#e63946' },
	'Sour Strip': { e: '🍋', bg: '#e9c400' },
};
const pic = (name: string) => CANDY[name] ?? { e: '🍬', bg: C.purple };

// Same picture for the rationing table, which only knows display names.
const DISPLAY_TO_RAW: Record<string, string> = {
	KitKat: 'KitKat',
	Starburst: 'Starbursts',
	"Hershey's Bar": "Hershey's",
	'Haribo Gummies': 'Haribo',
	"Reese's Peanut Butter Cup": "Reese's",
	Twizzlers: 'Twizzlers',
	Lollipop: 'Suckers',
	'Tootsie Roll': 'Tootsie Rolls',
	Twix: 'Twix',
	Warheads: 'Warheads',
	Skittles: 'Skittles',
	"M&M's Peanut": "M&M's Peanut",
	'Jolly Rancher Gummies': 'Jolly Ranchers Gu',
	'Assorted Hard Candy': 'Hard Candies',
	Snickers: 'Snickers',
	'3 Musketeers': '3 Musketeers',
	"M&M's Milk Chocolate": "M&M's Chocolate",
	'Almond Joy': 'Almond Joy',
	'Sour Patch Kids': 'Sour Patch Kids',
	'Milky Way': 'Milky Way',
	Airheads: 'Airheads',
	'Bubble Gum': 'Bubble',
	'Kinder Chocolate': 'Kinder',
	Butterfinger: 'Butterfinger',
	'Rice Krispies Treat': 'Rice Crispy',
	'Nerds Gummy Clusters': 'Nerds Gummy',
	SweeTarts: 'Sweettarts',
	Smarties: 'Smarties',
	'Milk Duds': 'Milk Duds',
	'Life Savers Gummies': 'Life Saver Gummie',
	'Sour Strips': 'Sour Strip',
};
const picByDisplay = (name: string) => pic(DISPLAY_TO_RAW[name] ?? name);

const CATEGORY: Record<string, { e: string; label: string }> = {
	chocolate: { e: '🍫', label: 'Chocolatey' },
	fruity: { e: '🍓', label: 'Fruity' },
	gummy: { e: '🐻', label: 'Gummy' },
	sour: { e: '🍋', label: 'Super sour' },
	hard_candy: { e: '🍭', label: 'Lollipops' },
	non_candy: { e: '🥔', label: 'Not candy!' },
	snack: { e: '🍪', label: 'Snacks' },
	gum: { e: '🫧', label: 'Bubble gum' },
};

const SHELVES = [
	{ v: 5, face: '😍', label: 'SUPER yummy!', color: '#ff4fa3' },
	{ v: 4, face: '😋', label: 'Yummy!', color: C.orange },
	{ v: 3, face: '🙂', label: 'Pretty good', color: '#f5c400' },
	{ v: 2, face: '😕', label: 'Meh…', color: '#8bc34a' },
	{ v: 1, face: '🤢', label: 'Trade it away!', color: C.slime },
];

type Tab = 'count' | 'race' | 'jar' | 'yummy' | 'grownups';
const TABS: { key: Tab; e: string; label: string }[] = [
	{ key: 'count', e: '🍬', label: 'Count my candy' },
	{ key: 'race', e: '🏆', label: 'Biggest piles' },
	{ key: 'jar', e: '🫙', label: 'Candy jar' },
	{ key: 'yummy', e: '😋', label: 'Yummy or yucky' },
	{ key: 'grownups', e: '🧑‍🍳', label: 'Grown-ups' },
];

const CSS = `
@keyframes ari-float { 0%,100% { transform: translateY(0) rotate(-4deg) } 50% { transform: translateY(-14px) rotate(4deg) } }
@keyframes ari-fly { 0% { transform: translateX(-80px) translateY(0) } 25% { transform: translateX(25vw) translateY(-18px) } 50% { transform: translateX(50vw) translateY(8px) } 75% { transform: translateX(75vw) translateY(-12px) } 100% { transform: translateX(110vw) translateY(0) } }
@keyframes ari-wiggle { 0%,100% { transform: rotate(0) scale(1) } 25% { transform: rotate(-8deg) scale(1.08) } 75% { transform: rotate(8deg) scale(1.08) } }
@keyframes ari-pop { 0% { transform: scale(0) rotate(-30deg); opacity: 0 } 70% { transform: scale(1.25) rotate(8deg); opacity: 1 } 100% { transform: scale(1) rotate(0); opacity: 1 } }
@keyframes ari-grow { from { transform: scaleX(0) } to { transform: scaleX(1) } }
@keyframes ari-twinkle { 0%,100% { opacity: .25 } 50% { opacity: 1 } }
@keyframes ari-glow { 0%,100% { text-shadow: 0 0 10px #ff7a00, 0 0 24px #ff7a0088 } 50% { text-shadow: 0 0 18px #ffb347, 0 0 40px #ff7a00cc } }
.ari-float { animation: ari-float 3.2s ease-in-out infinite }
.ari-fly { position: absolute; animation: ari-fly linear infinite; pointer-events: none }
.ari-tile { transition: transform .15s ease }
.ari-tile:hover { animation: ari-wiggle .5s ease-in-out }
.ari-tile:active { transform: scale(.92) }
.ari-pop { animation: ari-pop .45s cubic-bezier(.2,1.4,.4,1) both }
.ari-grow { transform-origin: left center; animation: ari-grow 1s cubic-bezier(.2,1,.3,1) both }
.ari-twinkle { animation: ari-twinkle 2.4s ease-in-out infinite }
.ari-glow { animation: ari-glow 2.5s ease-in-out infinite }
.ari-slider { width: 100%; accent-color: #ff7a00; height: 28px; cursor: pointer }
`;

// ── Little shared bits ────────────────────────────────────────────────

function Loading() {
	return (
		<div className="flex items-center justify-center gap-3 py-12 text-3xl" style={{ color: C.dim }}>
			<span className="ari-float">👻</span>
			<span className="text-lg">Ghosts are fetching the candy…</span>
		</div>
	);
}

function Oops({ error }: { error: unknown }) {
	return (
		<p role="alert" className="rounded-xl p-4 text-sm" style={{ background: '#00000055', color: '#ffb4a8' }}>
			🕷️ Oops, a spider got in the wires: {String(error)}
		</p>
	);
}

function BigButton({
	active,
	onClick,
	children,
	color = C.orange,
}: {
	active?: boolean;
	onClick: () => void;
	children: any;
	color?: string;
}) {
	return (
		<button
			onClick={onClick}
			className="ari-tile whitespace-nowrap rounded-2xl px-4 py-2 text-base font-bold"
			style={{
				cursor: 'pointer',
				fontFamily: FONT,
				background: active ? color : '#ffffff14',
				color: active ? C.night : C.white,
				border: `3px solid ${active ? '#fff' : '#ffffff33'}`,
				boxShadow: active ? `0 0 18px ${color}` : 'none',
			}}
		>
			{children}
		</button>
	);
}

// The spooky night sky behind everything.
function NightSky() {
	const stars = [
		[6, 8], [14, 30], [22, 12], [35, 22], [48, 6], [57, 28], [66, 14], [74, 4], [83, 24], [92, 10],
		[10, 55], [28, 70], [44, 62], [61, 78], [79, 58], [95, 72], [3, 88], [52, 92], [88, 90],
	];
	return (
		<div aria-hidden style={{ position: 'absolute', inset: 0, overflow: 'hidden', pointerEvents: 'none' }}>
			{stars.map(([x, y], i) => (
				<span
					key={i}
					className="ari-twinkle"
					style={{
						position: 'absolute',
						left: `${x}%`,
						top: `${y}%`,
						fontSize: 10,
						color: C.moon,
						animationDelay: `${(i % 6) * 0.4}s`,
					}}
				>
					✦
				</span>
			))}
			<div
				style={{
					position: 'absolute',
					right: 28,
					top: 18,
					width: 84,
					height: 84,
					borderRadius: '50%',
					background: `radial-gradient(circle at 35% 35%, #fffdf0, ${C.moon} 60%, #f3d36b)`,
					boxShadow: '0 0 60px #fff4c2aa',
				}}
			/>
			<span className="ari-fly" style={{ top: 40, fontSize: 28, animationDuration: '14s' }}>🦇</span>
			<span className="ari-fly" style={{ top: 110, fontSize: 20, animationDuration: '19s', animationDelay: '-7s' }}>🦇</span>
			<span className="ari-fly" style={{ top: 70, fontSize: 16, animationDuration: '23s', animationDelay: '-15s' }}>🦇</span>
			<span style={{ position: 'absolute', left: -6, top: -6, fontSize: 64, opacity: 0.35 }}>🕸️</span>
			<span style={{ position: 'absolute', left: 4, bottom: 0, fontSize: 44 }}>🎃</span>
			<span style={{ position: 'absolute', right: 6, bottom: 0, fontSize: 36 }}>🎃</span>
		</div>
	);
}

// ── Header: the big number ────────────────────────────────────────────

function Header() {
	const q = useSQLQuery(`
    SELECT total_pieces, candy_types, most_common_candy
    FROM "halloween_candy"."marts"."mart_haul_summary"
  `);
	const r = rowsOf(q)[0];

	return (
		<header className="relative mb-5 text-center" style={{ paddingTop: 8 }}>
			<p className="text-sm font-bold tracking-widest" style={{ color: C.pumpkin }}>
				✨ HALLOWEEN 2026 ✨
			</p>
			<h1 className="ari-glow text-4xl font-black" style={{ color: C.white, lineHeight: 1.15 }}>
				Ari's Spooky Candy Treasure!
			</h1>
			<div className="mt-3 flex items-center justify-center gap-4">
				<span className="ari-float text-5xl">👻</span>
				{q.isLoading ? (
					<div className="h-16 w-40 animate-pulse rounded-2xl" style={{ background: '#ffffff22' }} />
				) : q.isError ? (
					<Oops error={q.error} />
				) : (
					<div>
						<p className="font-black" style={{ fontSize: 72, lineHeight: 1, color: C.pumpkin }}>
							{N(r?.total_pieces)}
						</p>
						<p className="text-xl font-bold" style={{ color: C.white }}>
							treats in the bucket! 🪣
						</p>
					</div>
				)}
				<span className="ari-float text-5xl" style={{ animationDelay: '-1.6s' }}>
					🧛
				</span>
			</div>
		</header>
	);
}

// ── Tab 1: Count my candy ─────────────────────────────────────────────

function CountMyCandy({ haul }: { haul: any[] }) {
	const [picked, setPicked] = useDiveState<string | null>('picked', null);
	const candy = haul.find((r) => String(r.candy_name) === picked);

	return (
		<div>
			<p className="mb-3 text-center text-lg font-bold" style={{ color: C.white }}>
				👆 Tap a candy and count with me!
			</p>

			{candy && (
				<CountingStage
					key={picked ?? ''}
					raw={String(candy.candy_name)}
					name={String(candy.display_name)}
					count={N(candy.piece_count)}
					onClose={() => setPicked(null)}
				/>
			)}

			<div className="grid gap-3" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(96px, 1fr))' }}>
				{haul.map((r) => {
					const raw = String(r.candy_name);
					const p = pic(raw);
					const selected = raw === picked;
					return (
						<button
							key={raw}
							onClick={() => setPicked(raw)}
							className="ari-tile relative flex flex-col items-center rounded-2xl p-2"
							style={{
								cursor: 'pointer',
								fontFamily: FONT,
								background: p.bg,
								border: `3px solid ${selected ? '#fff' : '#00000033'}`,
								boxShadow: selected ? `0 0 20px #fff` : '0 4px 0 #00000055',
								color: '#fff',
							}}
						>
							<span style={{ fontSize: 40, lineHeight: 1.1 }}>{p.e}</span>
							<span
								className="mt-1 text-center text-xs font-bold"
								style={{ lineHeight: 1.1, textShadow: '0 1px 2px #0008', minHeight: 26 }}
							>
								{String(r.display_name)}
							</span>
							<span
								className="absolute flex items-center justify-center rounded-full text-sm font-black"
								style={{
									right: -6,
									top: -6,
									width: 28,
									height: 28,
									background: C.white,
									color: C.night,
									border: `2px solid ${C.orange}`,
								}}
							>
								{N(r.piece_count)}
							</span>
						</button>
					);
				})}
			</div>
		</div>
	);
}

function CountingStage({
	raw,
	name,
	count,
	onClose,
}: {
	raw: string;
	name: string;
	count: number;
	onClose: () => void;
}) {
	const p = pic(raw);
	const step = 0.28; // seconds between each piece popping in: slow enough to count along
	const cheer = count >= 10 ? 'WOW, a HUGE pile! 🤩' : count >= 5 ? 'That’s a lot! 🎉' : count === 1 ? 'Just one! 🫶' : 'Yum! 😋';

	return (
		<div
			className="mb-5 rounded-3xl p-4 text-center"
			style={{ background: '#00000055', border: `3px dashed ${C.pumpkin}` }}
		>
			<div className="flex items-center justify-between">
				<span />
				<p className="text-2xl font-black" style={{ color: C.white }}>
					{p.e} {name}
				</p>
				<button
					onClick={onClose}
					className="rounded-full px-3 py-1 text-sm font-bold"
					style={{ cursor: 'pointer', background: '#ffffff22', color: C.white, fontFamily: FONT }}
				>
					✖ close
				</button>
			</div>
			<div className="my-3 flex flex-wrap justify-center gap-2">
				{Array.from({ length: count }, (_, i) => (
					<div key={i} className="ari-pop flex flex-col items-center" style={{ animationDelay: `${i * step}s` }}>
						<span style={{ fontSize: 44, lineHeight: 1 }}>{p.e}</span>
						<span className="text-lg font-black" style={{ color: C.pumpkin }}>
							{i + 1}
						</span>
					</div>
				))}
			</div>
			<p
				className="ari-pop text-3xl font-black"
				style={{ color: C.slime, animationDelay: `${count * step + 0.2}s` }}
			>
				{count}! {cheer}
			</p>
		</div>
	);
}

// ── Tab 2: Biggest piles (a candy race) ───────────────────────────────

function CandyRace({ haul }: { haul: any[] }) {
	const top = haul.slice(0, 8);
	const max = Math.max(1, ...top.map((r) => N(r.piece_count)));
	const medals = ['🥇', '🥈', '🥉'];

	return (
		<div>
			<p className="mb-4 text-center text-lg font-bold" style={{ color: C.white }}>
				🏁 Which candy has the BIGGEST pile? 🏁
			</p>
			<div className="flex flex-col gap-2">
				{top.map((r, i) => {
					const raw = String(r.candy_name);
					const p = pic(raw);
					const n = N(r.piece_count);
					return (
						<div key={raw} className="flex items-center gap-2">
							<span className="text-center text-2xl" style={{ width: 34 }}>
								{medals[i] ?? ''}
							</span>
							<span className="text-right text-sm font-bold" style={{ width: 120, color: C.white }}>
								{String(r.display_name)}
							</span>
							<div
								className="ari-grow flex items-center rounded-full px-2"
								style={{
									width: `${Math.max(12, (n / max) * 100)}%`,
									maxWidth: 'calc(100% - 200px)',
									minHeight: 34,
									background: `linear-gradient(90deg, ${p.bg}, ${p.bg}cc)`,
									border: '2px solid #ffffff55',
									animationDelay: `${i * 0.12}s`,
									overflow: 'hidden',
									whiteSpace: 'nowrap',
								}}
							>
								<span style={{ fontSize: 18, letterSpacing: -2 }}>{p.e.repeat(n)}</span>
							</div>
							<span className="text-2xl font-black" style={{ color: C.pumpkin }}>
								{n}
							</span>
						</div>
					);
				})}
			</div>
			{top[0] && (
				<p className="mt-4 text-center text-base" style={{ color: C.dim }}>
					{pic(String(top[0].candy_name)).e} {String(top[0].display_name)} wins the race! 🎉
				</p>
			)}
		</div>
	);
}

function CategoryBuckets({ cats }: { cats: any[] }) {
	return (
		<div className="mt-6">
			<p className="mb-3 text-center text-lg font-bold" style={{ color: C.white }}>
				🪣 Candy buckets
			</p>
			<div className="grid grid-cols-4 gap-3">
				{cats.map((c) => {
					const key = String(c.category);
					const meta = CATEGORY[key] ?? { e: '🍬', label: key };
					return (
						<div
							key={key}
							className="ari-tile rounded-2xl p-2 text-center"
							style={{ background: '#ffffff12', border: '2px solid #ffffff22' }}
						>
							<div style={{ fontSize: 34 }}>{meta.e}</div>
							<div className="text-2xl font-black" style={{ color: C.pumpkin }}>
								{N(c.pieces)}
							</div>
							<div className="text-xs font-bold" style={{ color: C.white }}>
								{meta.label}
							</div>
						</div>
					);
				})}
			</div>
		</div>
	);
}

// ── Tab 3: Candy jar (how long will it last?) ─────────────────────────

function CandyJar() {
	const q = useSQLQuery(`
    SELECT eat_order, day_number, strftime(eat_date, '%a %b %-d') AS day_label, display_name
    FROM "halloween_candy"."marts"."mart_candy_rationing"
    ORDER BY eat_order
  `);
	const pieces = rowsOf(q).map((r) => ({
		order: N(r.eat_order),
		day: N(r.day_number),
		label: String(r.day_label),
		name: String(r.display_name),
	}));
	const lastDay = pieces.length ? pieces[pieces.length - 1].day : 0;
	const [day, setDay] = useDiveState<number>('jarDay', 0);
	const d = Math.min(Math.max(0, day), lastDay);

	if (q.isLoading) return <Loading />;
	if (q.isError) return <Oops error={q.error} />;

	const left = pieces.filter((p) => p.day > d);
	const today = pieces.filter((p) => p.day === d);
	const dayLabel = d === 0 ? 'Halloween night' : (today[0]?.label ?? '');
	const empty = left.length === 0;

	return (
		<div>
			<p className="mb-1 text-center text-lg font-bold" style={{ color: C.white }}>
				Ari gets <span style={{ color: C.pumpkin }}>3 treats a day</span>. Slide the pumpkin! 🎃
			</p>
			<p className="mb-3 text-center text-sm" style={{ color: C.dim }}>
				(After the grown-ups take their spooky chocolate tax 👻)
			</p>

			<div className="mb-3 flex items-center gap-3">
				<BigButton onClick={() => setDay(Math.max(0, d - 1))}>◀ 🌙</BigButton>
				<input
					aria-label="Day"
					className="ari-slider"
					type="range"
					min={0}
					max={lastDay}
					value={d}
					onChange={(e) => setDay(Number(e.target.value))}
				/>
				<BigButton onClick={() => setDay(Math.min(lastDay, d + 1))}>☀️ ▶</BigButton>
			</div>

			<div className="grid grid-cols-3 gap-4">
				{/* The jar */}
				<div style={{ gridColumn: 'span 2 / span 2' }}>
					<div
						className="mx-auto"
						style={{
							width: '78%',
							height: 18,
							borderRadius: '10px 10px 4px 4px',
							background: C.purple,
							border: '3px solid #ffffff66',
						}}
					/>
					<div
						className="mx-auto flex flex-wrap content-end justify-center gap-0.5 p-3"
						style={{
							width: '88%',
							height: 290,
							borderRadius: '18px 18px 48px 48px',
							background: 'linear-gradient(90deg, #ffffff10, #ffffff22 40%, #ffffff0c)',
							border: '4px solid #ffffff66',
							overflow: 'hidden',
						}}
					>
						{empty ? (
							<div className="flex h-full w-full flex-col items-center justify-center">
								<span style={{ fontSize: 56 }}>😱</span>
								<span className="text-xl font-black" style={{ color: C.white }}>
									All gone!
								</span>
							</div>
						) : (
							left
								.slice()
								.reverse()
								.map((p) => (
									<span key={p.order} style={{ fontSize: 23, lineHeight: 1.05 }}>
										{picByDisplay(p.name).e}
									</span>
								))
						)}
					</div>
					<p className="mt-2 text-center text-2xl font-black" style={{ color: C.pumpkin }}>
						{left.length} treats left!
					</p>
				</div>

				{/* Today */}
				<div className="rounded-3xl p-3 text-center" style={{ background: '#00000044', border: `2px dashed ${C.purple}` }}>
					<p className="text-sm font-bold" style={{ color: C.dim }}>
						{d === 0 ? '🎃' : `Day ${d}`}
					</p>
					<p className="text-lg font-black" style={{ color: C.white }}>
						{dayLabel}
					</p>
					{d === 0 ? (
						<p className="mt-4 text-base" style={{ color: C.white }}>
							Trick or treat! 🏃‍♂️🏠🏠🏠
							<br />
							<br />
							The jar is FULL!
						</p>
					) : (
						<>
							<p className="mt-3 text-sm font-bold" style={{ color: C.slime }}>
								Today Ari eats:
							</p>
							<div className="mt-2 flex flex-col gap-2">
								{today.map((p, i) => (
									<div key={p.order} className="ari-pop flex items-center gap-2" style={{ animationDelay: `${i * 0.15}s` }}>
										<span style={{ fontSize: 34 }}>{picByDisplay(p.name).e}</span>
										<span className="text-left text-sm font-bold" style={{ color: C.white }}>
											{p.name}
										</span>
									</div>
								))}
							</div>
						</>
					)}
					{d === lastDay && (
						<p className="mt-3 text-sm" style={{ color: C.pumpkin }}>
							Last candy day! 🥲
						</p>
					)}
				</div>
			</div>
		</div>
	);
}

// ── Tab 4: Yummy or yucky ─────────────────────────────────────────────

function YummyOrYucky({ haul }: { haul: any[] }) {
	const candy = haul.filter((r) => r.is_candy === true);
	return (
		<div>
			<p className="mb-4 text-center text-lg font-bold" style={{ color: C.white }}>
				Which candies are the yummiest? 🤔
			</p>
			<div className="flex flex-col gap-2">
				{SHELVES.map((s) => {
					const items = candy.filter((r) => N(r.trade_value) === s.v);
					return (
						<div
							key={s.v}
							className="flex items-center gap-3 rounded-2xl p-2"
							style={{ background: '#00000040', borderLeft: `8px solid ${s.color}` }}
						>
							<div className="text-center" style={{ width: 96 }}>
								<div style={{ fontSize: 38, lineHeight: 1 }}>{s.face}</div>
								<div className="text-xs font-black" style={{ color: s.color }}>
									{s.label}
								</div>
							</div>
							<div className="flex flex-1 flex-wrap gap-2">
								{items.map((r) => {
									const raw = String(r.candy_name);
									const p = pic(raw);
									return (
										<span
											key={raw}
											title={`${String(r.display_name)}: ${N(r.piece_count)}`}
											className="ari-tile flex items-center gap-1 rounded-full px-2 py-1 text-xs font-bold"
											style={{ background: p.bg, color: '#fff', textShadow: '0 1px 2px #0008' }}
										>
											<span style={{ fontSize: 20 }}>{p.e}</span>
											{String(r.display_name)}
											<span className="rounded-full px-1" style={{ background: '#0004' }}>
												×{N(r.piece_count)}
											</span>
										</span>
									);
								})}
							</div>
						</div>
					);
				})}
			</div>
			<p className="mt-3 text-center text-xs" style={{ color: C.dim }}>
				Yumminess is a grown-up guess. Ari gets the final say! 👑
			</p>
		</div>
	);
}

// ── Tab 5: Grown-ups ──────────────────────────────────────────────────

function GrownUps({ haul }: { haul: any[] }) {
	const s = useSQLQuery(`
    SELECT total_pieces, candy_types, pct_chocolate, parent_tax_pieces,
           est_total_sugar_kg, est_total_calories, non_candy_pieces
    FROM "halloween_candy"."marts"."mart_haul_summary"
  `);
	const m = useSQLQuery(`
    SELECT manufacturer, pieces, market_share_pct
    FROM "halloween_candy"."marts"."mart_manufacturer_summary"
    ORDER BY pieces DESC
    LIMIT 5
  `);
	const r = rowsOf(s)[0];
	const nuts = haul.filter((x) => x.has_peanuts === true || x.has_tree_nuts === true);
	const taxed = haul.filter((x) => N(x.parent_tax_pieces) > 0);

	const card = { background: '#00000044', border: '2px solid #ffffff22' };

	return (
		<div className="grid grid-cols-2 gap-4" style={{ color: C.white }}>
			<div className="rounded-2xl p-3" style={card}>
				<p className="mb-2 font-black" style={{ color: C.pumpkin }}>
					📊 The numbers
				</p>
				{s.isLoading ? (
					<Loading />
				) : s.isError ? (
					<Oops error={s.error} />
				) : (
					<ul className="space-y-1 text-sm">
						<li>🍬 {N(r?.total_pieces)} pieces, {N(r?.candy_types)} kinds</li>
						<li>🍫 {N(r?.pct_chocolate).toFixed(1)}% chocolate</li>
						<li>🥔 {N(r?.non_candy_pieces)} non-candy items (chips, Play-Doh, granola)</li>
						<li>🧪 ~{N(r?.est_total_sugar_kg).toFixed(2)} kg sugar · ~{N(r?.est_total_calories).toLocaleString()} kcal (estimates)</li>
					</ul>
				)}
			</div>

			<div className="rounded-2xl p-3" style={card}>
				<p className="mb-2 font-black" style={{ color: C.pumpkin }}>
					👻 Parent tax: {r ? N(r.parent_tax_pieces) : '…'} pieces
				</p>
				<div className="flex flex-wrap gap-2 text-sm">
					{taxed.map((x) => (
						<span key={String(x.candy_name)}>
							{pic(String(x.candy_name)).e} {String(x.display_name)} ×{N(x.parent_tax_pieces)}
						</span>
					))}
				</div>
				<p className="mt-2 text-xs" style={{ color: C.dim }}>
					10% of each chocolate, rounded. Change it with <code>--vars '{'{parent_tax_pct: 0.25}'}'</code>
				</p>
			</div>

			<div className="rounded-2xl p-3" style={card}>
				<p className="mb-2 font-black" style={{ color: C.pumpkin }}>
					🥜 Has nuts (check wrappers!)
				</p>
				<div className="flex flex-wrap gap-2 text-sm">
					{nuts.map((x) => (
						<span key={String(x.candy_name)}>
							{pic(String(x.candy_name)).e} {String(x.display_name)} ×{N(x.piece_count)}
							{x.has_tree_nuts === true ? ' (tree nut)' : ''}
						</span>
					))}
				</div>
			</div>

			<div className="rounded-2xl p-3" style={card}>
				<p className="mb-2 font-black" style={{ color: C.pumpkin }}>
					🏭 Candy makers
				</p>
				{m.isLoading ? (
					<Loading />
				) : m.isError ? (
					<Oops error={m.error} />
				) : (
					<table className="w-full text-sm">
						<tbody>
							{rowsOf(m).map((x) => (
								<tr key={String(x.manufacturer)}>
									<td className="py-0.5">{String(x.manufacturer)}</td>
									<td className="py-0.5 text-right">{N(x.pieces)}</td>
									<td className="py-0.5 text-right" style={{ color: C.dim }}>
										{N(x.market_share_pct).toFixed(0)}%
									</td>
								</tr>
							))}
						</tbody>
					</table>
				)}
			</div>
		</div>
	);
}

// ── Page ──────────────────────────────────────────────────────────────

export default function SpookyCandyTreasure() {
	const [tab, setTab] = useDiveState<Tab>('tab', 'count');

	const haulQ = useSQLQuery(`
    SELECT candy_name, display_name, category, piece_count, trade_value, is_candy,
           has_peanuts, has_tree_nuts, parent_tax_pieces
    FROM "halloween_candy"."marts"."fct_candy_haul"
    ORDER BY piece_count DESC, display_name
  `);
	const catQ = useSQLQuery(`
    SELECT category, pieces
    FROM "halloween_candy"."marts"."mart_category_summary"
    ORDER BY pieces DESC
  `);
	const haul = rowsOf(haulQ);

	const needsHaul = tab !== 'jar';

	return (
		<main
			className="relative p-5"
			style={{
				minHeight: '100%',
				fontFamily: FONT,
				color: C.white,
				background: `radial-gradient(ellipse at 80% 0%, ${C.night2}, ${C.night} 60%), ${C.night}`,
				overflow: 'hidden',
			}}
		>
			<style>{CSS}</style>
			<NightSky />

			<div className="relative">
				<Header />

				<nav className="mb-5 flex flex-wrap justify-center gap-2" role="tablist">
					{TABS.map((t) => (
						<BigButton key={t.key} active={tab === t.key} onClick={() => setTab(t.key)}>
							<span style={{ fontSize: 20 }}>{t.e}</span> {t.label}
						</BigButton>
					))}
				</nav>

				<section className="rounded-3xl p-4" style={{ background: '#ffffff0d', border: '2px solid #ffffff1f' }}>
					{needsHaul && haulQ.isLoading ? (
						<Loading />
					) : needsHaul && haulQ.isError ? (
						<Oops error={haulQ.error} />
					) : tab === 'count' ? (
						<CountMyCandy haul={haul} />
					) : tab === 'race' ? (
						<>
							<CandyRace haul={haul} />
							{catQ.isError ? <Oops error={catQ.error} /> : <CategoryBuckets cats={rowsOf(catQ)} />}
						</>
					) : tab === 'jar' ? (
						<CandyJar />
					) : tab === 'yummy' ? (
						<YummyOrYucky haul={haul} />
					) : (
						<GrownUps haul={haul} />
					)}
				</section>

				<p className="mt-4 text-center text-xs" style={{ color: C.dim }}>
					Happy Halloween, Ari! 🎃 Love, the candy ghosts 👻
				</p>
			</div>
		</main>
	);
}
