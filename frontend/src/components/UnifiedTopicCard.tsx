'use client';

import type { ReactNode } from 'react';
import Link from 'next/link';
import { Clock } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { cn } from '@/lib/utils';

export interface UnifiedTopicMention {
  stock_code: string;
  stock_name: string;
  return_1d?: number | null;
  return_3d?: number | null;
  return_5d?: number | null;
  return_10d?: number | null;
  return_20d?: number | null;
}

interface UnifiedTopicCardProps {
  scope: 'group' | 'global';
  topicId: string | number;
  createTime?: string;
  groupId?: string | number;
  groupName?: string;
  type?: string;
  likesCount?: number;
  commentsCount?: number;
  readingCount?: number;
  titleHtml?: { __html: string };
  contentHtml?: { __html: string };
  expanded?: boolean;
  showExpandButton?: boolean;
  onToggleExpand?: () => void;
  mentions?: UnifiedTopicMention[];
  onMentionClick?: (stockCode: string) => void;
  headerSlot?: ReactNode;
  footerSlot?: ReactNode;
  children?: ReactNode;
  className?: string;
  contentClassName?: string;
  hideMetaHeader?: boolean;
  clampTitleWhenCollapsed?: boolean;
  clampContentWhenCollapsed?: boolean;
}

const fmtPct = (v: number | null | undefined) => {
  if (v == null) return '—';
  const sign = v > 0 ? '+' : '';
  return `${sign}${v.toFixed(2)}%`;
};

const pctColor = (v: number | null | undefined) => {
  if (v == null) return 'text-muted-foreground';
  if (v > 0) return 'text-emerald-500';
  if (v < 0) return 'text-red-500';
  return 'text-muted-foreground';
};

export default function UnifiedTopicCard({
  scope,
  topicId,
  createTime,
  groupId,
  groupName,
  type,
  likesCount,
  commentsCount,
  readingCount,
  titleHtml,
  contentHtml,
  expanded,
  showExpandButton,
  onToggleExpand,
  mentions = [],
  onMentionClick,
  headerSlot,
  footerSlot,
  children,
  className,
  contentClassName,
  hideMetaHeader = false,
  clampTitleWhenCollapsed = true,
  clampContentWhenCollapsed = true,
}: UnifiedTopicCardProps) {
  return (
    <Card className={cn('overflow-hidden hover:border-primary/20 transition-colors', className)}>
      <CardContent className={cn('p-4 space-y-3', contentClassName)}>
        {!hideMetaHeader && (
          <>
            <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
              <div className="flex items-center gap-2">
                <Clock className="h-3 w-3" />
                <span>{createTime || '—'}</span>
              </div>
              <Badge variant="outline" className="font-mono text-[10px]">
                topic {topicId}
              </Badge>
            </div>

            {scope === 'global' && (
              <div className="text-xs text-muted-foreground">
                <span>群组: </span>
                {groupId ? (
                  <Link
                    href={`/groups/${groupId}`}
                    className="underline underline-offset-2 hover:text-primary"
                  >
                    {groupName || groupId}
                  </Link>
                ) : (
                  <span>{groupName || '—'}</span>
                )}
              </div>
            )}

            {(type || likesCount != null || commentsCount != null || readingCount != null) && (
              <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
                {type && (
                  <Badge variant="secondary" className="text-[10px]">
                    {type}
                  </Badge>
                )}
                <span>👍 {likesCount || 0}</span>
                <span>💬 {commentsCount || 0}</span>
                <span>👀 {readingCount || 0}</span>
              </div>
            )}
          </>
        )}

        {headerSlot}

        {titleHtml && (
          <div
            className={cn(
              'text-sm font-medium text-foreground/90 break-words prose prose-sm max-w-none prose-p:my-1 prose-strong:text-gray-900 prose-a:text-blue-600',
              !expanded && clampTitleWhenCollapsed ? 'line-clamp-2' : ''
            )}
            style={{ wordBreak: 'break-all', overflowWrap: 'anywhere' }}
            dangerouslySetInnerHTML={titleHtml}
          />
        )}

        {children ? (
          children
        ) : contentHtml ? (
          <div className="w-full">
            <div
              className="bg-gray-50 rounded-lg p-3 w-full max-w-full overflow-hidden"
              style={{ minWidth: 0 }}
            >
              <div
                className={cn(
                  'text-sm text-gray-800 whitespace-pre-wrap break-words break-all prose prose-sm max-w-none prose-p:my-1 prose-strong:text-gray-900 prose-a:text-blue-600',
                  !expanded && clampContentWhenCollapsed ? 'line-clamp-8' : ''
                )}
                style={{ wordBreak: 'break-all', overflowWrap: 'anywhere' }}
                dangerouslySetInnerHTML={contentHtml}
              />
            </div>
          </div>
        ) : null}

        {showExpandButton && onToggleExpand && (
          <div className="flex justify-end">
            <Button size="sm" variant="ghost" className="h-6 px-2 text-xs" onClick={onToggleExpand}>
              {expanded ? '收起' : '展开全部'}
            </Button>
          </div>
        )}

        {(mentions || []).length > 0 && (
          <div className="pt-2 border-t border-border/50">
            <div className="flex flex-col gap-2">
              {(mentions || []).map((m, idx) => (
                <button
                  type="button"
                  key={`${topicId}-${m.stock_code}-${idx}`}
                  className="flex items-center gap-3 bg-muted/20 p-2 rounded-md text-left hover:bg-muted/40 transition-colors"
                  onClick={() => onMentionClick?.(m.stock_code)}
                >
                  <div className="min-w-[140px]">
                    <Badge variant="outline" className="font-normal bg-background">
                      {m.stock_name || '未知股票'}
                      <span className="ml-1 opacity-50 text-[10px]">{m.stock_code}</span>
                    </Badge>
                  </div>
                  <div className="flex-1 grid grid-cols-5 gap-2 text-xs">
                    <div className="flex flex-col items-center">
                      <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+1</span>
                      <span className={`font-mono font-medium ${pctColor(m.return_1d)}`}>
                        {fmtPct(m.return_1d)}
                      </span>
                    </div>
                    <div className="flex flex-col items-center">
                      <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+3</span>
                      <span className={`font-mono font-medium ${pctColor(m.return_3d)}`}>
                        {fmtPct(m.return_3d)}
                      </span>
                    </div>
                    <div className="flex flex-col items-center">
                      <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+5</span>
                      <span className={`font-mono font-medium ${pctColor(m.return_5d)}`}>
                        {fmtPct(m.return_5d)}
                      </span>
                    </div>
                    <div className="flex flex-col items-center">
                      <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+10</span>
                      <span className={`font-mono font-medium ${pctColor(m.return_10d)}`}>
                        {fmtPct(m.return_10d)}
                      </span>
                    </div>
                    <div className="flex flex-col items-center">
                      <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+20</span>
                      <span className={`font-mono font-medium ${pctColor(m.return_20d)}`}>
                        {fmtPct(m.return_20d)}
                      </span>
                    </div>
                  </div>
                </button>
              ))}
            </div>
          </div>
        )}

        {footerSlot}
      </CardContent>
    </Card>
  );
}
