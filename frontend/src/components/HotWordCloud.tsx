import React, { useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import 'echarts-wordcloud';
import { Flame } from 'lucide-react';

export interface HotWordData {
    name: string;
    value: number;
    raw_count?: number;
    normalized_count?: number;
}

interface HotWordCloudProps {
    words: HotWordData[];
    loading?: boolean;
    /** 排行榜展示的数量，默认 10 */
    rankCount?: number;
    emptyText?: string;
    onWordClick?: (word: string) => void;
}

interface WordCloudTooltipParam {
    name?: string;
    value?: number;
    data?: HotWordData;
}

interface WordCloudColorParam {
    dataIndex?: number;
}

/** 基于排名的确定性渐变色（热→冷：红橙 → 蓝紫） */
function rankColor(index: number, total: number): string {
    const ratio = total <= 1 ? 0 : index / (total - 1);
    // HSL: 0(红) → 240(蓝紫) 渐变
    const hue = Math.round(ratio * 240);
    const sat = Math.round(78 - ratio * 18);
    const light = Math.round(44 + ratio * 12);
    return `hsl(${hue}, ${sat}%, ${light}%)`;
}

export default function HotWordCloud({
    words,
    loading = false,
    rankCount = 10,
    emptyText = '暂无相关热词数据',
    onWordClick,
}: HotWordCloudProps) {
    // 确保 words 按 value 降序排列
    const sortedWords = useMemo(
        () => (words ? [...words].sort((a, b) => b.value - a.value) : []),
        [words],
    );

    // 稳定化 echarts option，避免 React 重渲染导致词云闪动
    const chartOption = useMemo(() => {
        if (!sortedWords.length) return null;
        const total = sortedWords.length;

        // 使用 sqrt 缩小最高词和最低词的大小差异，避免冷门词消失
        const chartData = sortedWords.map(w => ({
            ...w,
            value: Math.sqrt(w.value || 0),
            _originalValue: w.value
        }));

        return {
            tooltip: {
                show: true,
                formatter: (params: WordCloudTooltipParam & { data?: { _originalValue?: number } }) => {
                    const raw = params?.data?.raw_count;
                    const norm = params?.data?.normalized_count ?? params.data?._originalValue ?? params.value;
                    const rawLine = typeof raw === 'number' ? `<br/>原始 <b>${raw}</b> 次` : '';
                    return `<span style="font-weight:600">${params.name}</span><br/>折算日均 <b>${norm}</b>${rawLine}`;
                },
                backgroundColor: 'rgba(15,23,42,.88)',
                borderColor: 'transparent',
                textStyle: { color: '#f1f5f9', fontSize: 13 },
                padding: [8, 12],
            },
            series: [
                {
                    type: 'wordCloud',
                    shape: 'circle',
                    keepAspect: false,
                    left: 'center',
                    top: 'center',
                    width: '96%',
                    height: '96%',
                    right: null,
                    bottom: null,
                    sizeRange: [13, 54],
                    // 全部水平排列，消除随机旋转
                    rotationRange: [0, 0],
                    rotationStep: 0,
                    gridSize: 6,
                    drawOutOfBound: false,
                    // 关闭布局动画，消除闪动
                    layoutAnimation: false,
                    textStyle: {
                        fontFamily:
                            '"Inter", "PingFang SC", "Microsoft YaHei", system-ui, sans-serif',
                        fontWeight: 'bold',
                        // 基于数据索引的确定性颜色
                        color: function (params: WordCloudColorParam) {
                            const idx = params.dataIndex ?? 0;
                            return rankColor(idx, total);
                        },
                    },
                    emphasis: {
                        focus: 'self',
                        textStyle: {
                            shadowBlur: 6,
                            shadowColor: 'rgba(0,0,0,.25)',
                        },
                    },
                    data: chartData,
                },
            ],
        };
    }, [sortedWords]);

    // --- 加载态 ---
    if (loading) {
        return (
            <div className="w-full h-full flex flex-col items-center justify-center bg-slate-50/50 rounded-lg min-h-[220px]">
                <div className="w-5 h-5 rounded-full border-2 border-primary border-t-transparent animate-spin mb-2"></div>
                <div className="text-xs text-muted-foreground">加载热词中...</div>
            </div>
        );
    }

    // --- 空态 ---
    if (!sortedWords.length) {
        return (
            <div className="w-full h-full flex flex-col items-center justify-center bg-slate-50/50 rounded-lg min-h-[220px]">
                <span className="text-sm text-muted-foreground">{emptyText}</span>
            </div>
        );
    }

    const topWords = sortedWords.slice(0, rankCount);
    const maxValue = topWords[0]?.value ?? 1;

    return (
        <div className="w-full flex flex-col gap-3">
            {/* 词云 */}
            <div className="w-full min-h-[220px]">
                {chartOption && (
                    <ReactECharts
                        option={chartOption}
                        style={{ height: '220px', width: '100%' }}
                        opts={{ renderer: 'canvas' }}
                        notMerge={true}
                        onEvents={{
                            click: (params: { name?: string }) => {
                                const word = String(params?.name || '').trim();
                                if (word) onWordClick?.(word);
                            },
                        }}
                    />
                )}
            </div>

            {/* Top 热词排行榜 */}
            <div className="w-full">
                <div className="flex items-center gap-1.5 mb-2 px-0.5">
                    <Flame className="h-3.5 w-3.5 text-orange-500" />
                    <span className="text-xs font-medium text-gray-600">
                        热词前 {Math.min(rankCount, topWords.length)} (日均频率测算)
                    </span>
                </div>
                <div className="space-y-1">
                    {topWords.map((w, i) => {
                        const pct = maxValue > 0 ? (w.value / maxValue) * 100 : 0;
                        const color = rankColor(i, sortedWords.length);
                        return (
                            <button
                                key={w.name}
                                type="button"
                                className="flex w-full items-center gap-2 text-xs group text-left hover:bg-gray-50 rounded px-1 py-0.5 transition-colors"
                                onClick={() => onWordClick?.(w.name)}
                                title={`搜索：${w.name}`}
                            >
                                {/* 排名 */}
                                <span
                                    className="flex-shrink-0 w-4 text-right font-mono font-semibold"
                                    style={{ color: i < 3 ? color : '#9ca3af' }}
                                >
                                    {i + 1}
                                </span>
                                {/* 名称+柱状条 */}
                                <div className="flex-1 min-w-0 flex items-center gap-1.5">
                                    <span className="truncate font-medium text-gray-800 max-w-[90px]">
                                        {w.name}
                                    </span>
                                    <div className="flex-1 h-[6px] rounded-full bg-gray-100 overflow-hidden">
                                        <div
                                            className="h-full rounded-full transition-all duration-300"
                                            style={{
                                                width: `${pct}%`,
                                                backgroundColor: color,
                                                opacity: 0.75,
                                            }}
                                        />
                                    </div>
                                </div>
                                {/* 数值 */}
                                <span className="flex-shrink-0 font-mono text-gray-500 text-right tabular-nums leading-tight">
                                    <span className="block">{w.value}</span>
                                    {typeof w.raw_count === 'number' && (
                                        <span className="block text-[10px] text-gray-400">原始 {w.raw_count}</span>
                                    )}
                                </span>
                            </button>
                        );
                    })}
                </div>
            </div>
        </div>
    );
}
