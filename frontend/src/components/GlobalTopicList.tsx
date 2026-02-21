'use client';

import { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import ImageGallery from '@/components/ImageGallery';
import Link from 'next/link';
import { apiClient } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { createSafeHtmlWithHighlight, extractPlainText } from '@/lib/zsxq-content-renderer';
import UnifiedTopicCard from '@/components/UnifiedTopicCard';

interface GlobalTopicListProps {
  searchTerm?: string;
  onStatsUpdate?: (stats: { whitelistGroupCount: number; total: number }) => void;
  onMentionClick?: (stockCode: string) => void;
}

interface TopicMention {
  stock_code: string;
  stock_name: string;
  return_1d?: number | null;
  return_3d?: number | null;
  return_5d?: number | null;
  return_10d?: number | null;
  return_20d?: number | null;
}

interface GlobalTopicItem {
  group_id: string;
  group_name: string;
  topic_id: string;
  type?: string;
  title?: string;
  comments_count?: number;
  likes_count?: number;
  reading_count?: number;
  create_time?: string;
  text?: string;
  question_text?: string;
  answer_text?: string;
  talk_text?: string;
  mentions: TopicMention[];
}

const PAGE_SIZE = 50;

export default function GlobalTopicList({ searchTerm, onStatsUpdate, onMentionClick }: GlobalTopicListProps) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [items, setItems] = useState<GlobalTopicItem[]>([]);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [whitelistGroupCount, setWhitelistGroupCount] = useState(0);
  const [expandedTopics, setExpandedTopics] = useState<Set<string>>(new Set());
  const [topicDetails, setTopicDetails] = useState<Map<string, any>>(new Map());
  const inFlightRef = useRef<Map<string, Promise<any>>>(new Map());

  const normalizedSearch = useMemo(() => (searchTerm || '').trim(), [searchTerm]);
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const loadTopics = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await apiClient.getGlobalTopics(page, PAGE_SIZE, normalizedSearch || undefined);
      setItems(res?.items || []);
      const newTotal = res?.total || 0;
      const newWhitelist = res?.whitelist_group_count || 0;
      setTotal(newTotal);
      setWhitelistGroupCount(newWhitelist);
      if (onStatsUpdate) {
        onStatsUpdate({ whitelistGroupCount: newWhitelist, total: newTotal });
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载失败');
    } finally {
      setLoading(false);
    }
  }, [page, normalizedSearch]);

  useEffect(() => {
    void loadTopics();
  }, [loadTopics]);

  useEffect(() => {
    if (!items || items.length === 0) return;
    items.forEach(topic => {
      const tid = String(topic.topic_id);
      const gid = String(topic.group_id);
      if (!tid) return;
      const topicKey = `${gid}-${tid}`;
      if (topicDetails.has(topicKey)) return;

      if (inFlightRef.current.has(topicKey)) return;

      const p = apiClient.getTopicDetail(tid, gid)
        .then(detail => {
          setTopicDetails(prev => {
            const next = new Map(prev);
            next.set(topicKey, detail);
            return next;
          });
        })
        .catch(err => console.error('预取详情失败:', err))
        .finally(() => {
          inFlightRef.current.delete(topicKey);
        });

      inFlightRef.current.set(topicKey, p);
    });
  }, [items, topicDetails]);

  useEffect(() => {
    setPage(1);
    setExpandedTopics(new Set());
  }, [normalizedSearch]);

  return (
    <div className="h-full flex flex-col min-h-0">
      <div className="flex-1 overflow-y-auto min-h-0 pr-2">
        {loading ? (
          <div className="py-12 text-center text-sm text-muted-foreground">加载话题中...</div>
        ) : error ? (
          <div className="py-12 text-center text-sm text-red-500">{error}</div>
        ) : items.length === 0 ? (
          <div className="py-12 text-center text-sm text-muted-foreground">
            {normalizedSearch ? '没有匹配的话题' : '白名单群组下暂无可展示话题'}
          </div>
        ) : (
          <div className="space-y-3">
            {items.map((topic) => {
              const topicKey = `${topic.group_id}-${topic.topic_id}`;
              const detail = topicDetails.get(topicKey);
              const isExpanded = expandedTopics.has(topicKey);
              const detailQuestion = String(detail?.question?.text || topic.question_text || '');
              const detailAnswer = String(detail?.answer?.text || topic.answer_text || '');
              const detailTalk = String(detail?.talk?.text || topic.talk_text || '');
              const contentText = detailTalk || [detailQuestion, detailAnswer].filter(Boolean).join('\n\n') || String(topic.text || '');
              const isQaTopic = topic.type === 'q&a' || Boolean(detailQuestion || detailAnswer);

              const plainContent = extractPlainText(contentText);
              const contentLines = plainContent.split('\n').length;
              const needExpand = contentLines > 30 || plainContent.length > 1500;
              const toVisibleText = (raw: string) => {
                if (isExpanded || !needExpand) return raw;
                const plain = extractPlainText(raw);

                const lines = plain.split('\n');
                if (lines.length > 30) {
                  return lines.slice(0, 30).join('\n') + '\n...';
                }
                return plain.length > 1500 ? `${plain.slice(0, 1500)}...` : plain;
              };

              const visibleQuestion = toVisibleText(detailQuestion);
              const visibleAnswer = toVisibleText(detailAnswer);
              const visibleTalk = toVisibleText(detailTalk || contentText);
              return (
                <UnifiedTopicCard
                  key={`${topic.group_id}-${topic.topic_id}`}
                  scope="global"
                  topicId={topic.topic_id}
                  createTime={topic.create_time}
                  groupId={topic.group_id}
                  groupName={topic.group_name || topic.group_id}
                  type={topic.type || 'talk'}
                  likesCount={topic.likes_count || 0}
                  commentsCount={topic.comments_count || 0}
                  readingCount={topic.reading_count || 0}
                  titleHtml={undefined}
                  contentHtml={undefined}
                  expanded={isExpanded}
                  showExpandButton={needExpand}
                  clampTitleWhenCollapsed={false}
                  clampContentWhenCollapsed={false}
                  onToggleExpand={() => {
                    setExpandedTopics((prev) => {
                      const next = new Set(prev);
                      if (next.has(topicKey)) next.delete(topicKey);
                      else next.add(topicKey);
                      return next;
                    });
                  }}
                  mentions={topic.mentions || []}
                  onMentionClick={onMentionClick}
                  footerSlot={(topic.mentions || []).length === 0 ? (
                    <div className="pt-2 border-t border-border/50 text-xs text-muted-foreground">
                      该话题暂无股票提及记录
                    </div>
                  ) : undefined}
                >
                  <div className="space-y-3">
                    {isQaTopic ? (
                      <div className="space-y-3">
                        {detailQuestion && (
                          <div className="space-y-1.5">
                            <div className="text-sm text-gray-600">
                              <span className="font-medium">
                                {detail?.question?.anonymous ? '匿名用户' : (detail?.question?.owner?.name || '用户')} 提问：
                              </span>
                            </div>
                            <div className="bg-gray-50 border-l-4 border-gray-300 pl-4 py-3 rounded-r-lg">
                              <div
                                className="text-sm text-gray-500 whitespace-pre-wrap break-words break-all leading-relaxed prose prose-sm max-w-none prose-p:my-1 prose-strong:text-gray-700 prose-a:text-blue-500"
                                style={{ wordBreak: 'break-all', overflowWrap: 'anywhere' }}
                                dangerouslySetInnerHTML={createSafeHtmlWithHighlight(visibleQuestion, normalizedSearch || undefined)}
                              />
                            </div>
                          </div>
                        )}

                        {(detailAnswer || contentText) && (
                          <div className="w-full">
                            <div className="bg-gray-50 rounded-lg p-3 w-full max-w-full overflow-hidden" style={{ minWidth: 0 }}>
                              <div
                                className="text-sm text-gray-800 whitespace-pre-wrap break-words break-all prose prose-sm max-w-none prose-p:my-1 prose-strong:text-gray-900 prose-a:text-blue-600"
                                style={{ wordBreak: 'break-all', overflowWrap: 'anywhere' }}
                                dangerouslySetInnerHTML={createSafeHtmlWithHighlight(visibleAnswer || toVisibleText(contentText), normalizedSearch || undefined)}
                              />
                            </div>
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="w-full">
                        <div className="bg-gray-50 rounded-lg p-3 w-full max-w-full overflow-hidden" style={{ minWidth: 0 }}>
                          <div
                            className="text-sm text-gray-800 whitespace-pre-wrap break-words break-all prose prose-sm max-w-none prose-p:my-1 prose-strong:text-gray-900 prose-a:text-blue-600"
                            style={{ wordBreak: 'break-all', overflowWrap: 'anywhere' }}
                            dangerouslySetInnerHTML={createSafeHtmlWithHighlight(visibleTalk, normalizedSearch || undefined)}
                          />
                        </div>
                      </div>
                    )}
                  </div>


                  {/* 话题图片 */}
                  {detail?.talk?.images && detail.talk.images.length > 0 && (
                    <div className="mt-2 w-full max-w-full">
                      <ImageGallery
                        images={detail.talk.images}
                        className="w-full max-w-full"
                        groupId={topic.group_id.toString()}
                      />
                    </div>
                  )}

                  {/* 话题文件 */}
                  {detail?.talk?.files && detail.talk.files.length > 0 && (
                    <div className="mt-2 text-xs text-muted-foreground flex gap-1 flex-col">
                      {detail.talk.files.map((f: any) => (
                        <div key={f.file_id} className="bg-muted/10 p-2 rounded border border-border/50">
                          📎 {f.name} (请前往单群格式查看附带文件)
                        </div>
                      ))}
                    </div>
                  )}

                  {/* 话题评论 */}
                  {detail?.show_comments && detail.show_comments.length > 0 && (
                    <div className="mt-3 space-y-2 pt-2 border-t border-border/20">
                      <h4 className="text-xs font-medium text-foreground/80">
                        精选评论 ({detail.comments_count || 0})
                      </h4>
                      {detail.show_comments.slice(0, 3).map((comment: any) => (
                        <div key={comment.comment_id} className="bg-muted/30 rounded-md p-2">
                          <div className="flex items-center gap-2 mb-1">
                            {comment.owner?.avatar_url ? (
                              <img
                                src={apiClient.getProxyImageUrl(comment.owner.avatar_url, topic.group_id.toString())}
                                alt={comment.owner.name}
                                loading="lazy"
                                className="w-4 h-4 rounded-full object-cover block"
                                onError={(e) => { e.currentTarget.src = '/default-avatar.png'; }}
                              />
                            ) : (
                              <div className="w-4 h-4 rounded-full bg-muted block" />
                            )}
                            <span className="text-xs font-medium text-foreground/70">{comment.owner?.name}</span>
                            {/* 显示回复关系 */}
                            {comment.repliee && (
                              <>
                                <span className="text-xs text-muted-foreground/60">回复</span>
                                <span className="text-xs font-medium text-primary/80">
                                  {comment.repliee.name}
                                </span>
                              </>
                            )}
                          </div>
                          <div
                            className="text-xs text-muted-foreground ml-6 break-words prose prose-xs max-w-none prose-a:text-blue-500"
                            dangerouslySetInnerHTML={createSafeHtmlWithHighlight(comment.text, normalizedSearch || undefined)}
                          />
                          {comment.images && comment.images.length > 0 && (
                            <div className="ml-6 mt-1">
                              <ImageGallery
                                images={comment.images}
                                size="small"
                                groupId={topic.group_id.toString()}
                              />
                            </div>
                          )}

                          {/* 嵌套回复评论（二级评论） */}
                          {comment.replied_comments && comment.replied_comments.length > 0 && (
                            <div className="ml-6 mt-2 space-y-2 border-l-2 border-border/50 pl-3">
                              {comment.replied_comments.map((reply: any) => (
                                <div key={reply.comment_id} className="bg-background rounded p-2">
                                  <div className="flex items-center gap-2 mb-1">
                                    {reply.owner?.avatar_url && (
                                      <img
                                        src={apiClient.getProxyImageUrl(reply.owner.avatar_url, topic.group_id.toString())}
                                        alt={reply.owner.name}
                                        loading="lazy"
                                        className="w-3 h-3 rounded-full object-cover block"
                                        onError={(e) => { (e.currentTarget as HTMLImageElement).style.display = 'none'; }}
                                      />
                                    )}
                                    <span className="text-xs font-medium text-foreground/70">
                                      {reply.owner?.name || '未知用户'}
                                    </span>
                                    {reply.repliee && (
                                      <>
                                        <span className="text-xs text-muted-foreground/60">回复</span>
                                        <span className="text-xs font-medium text-primary/80">
                                          {reply.repliee.name}
                                        </span>
                                      </>
                                    )}
                                  </div>
                                  <div
                                    className="text-xs text-muted-foreground ml-5 break-words prose prose-xs max-w-none prose-a:text-blue-500"
                                    dangerouslySetInnerHTML={createSafeHtmlWithHighlight(reply.text || '', normalizedSearch || undefined)}
                                  />
                                  {/* 嵌套回复图片 */}
                                  {reply.images && reply.images.length > 0 && (
                                    <div className="ml-5 mt-1">
                                      <ImageGallery
                                        images={reply.images}
                                        size="small"
                                        groupId={topic.group_id.toString()}
                                      />
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ))}
                      {detail.show_comments.length > 3 && (
                        <div className="text-center pt-1">
                          <Link href={`/groups/${topic.group_id}`} className="text-[10px] text-primary hover:underline">
                            前往群组查看全部 {detail.comments_count} 条评论
                          </Link>
                        </div>
                      )}
                    </div>
                  )}

                </UnifiedTopicCard>
              );
            })}
          </div>
        )}
      </div>

      {totalPages > 1 && (
        <div className="flex-shrink-0 flex items-center justify-center gap-3 pt-3 border-t border-gray-200 mt-2 pb-0">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
          >
            上一页
          </Button>

          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-600">第</span>
            <input
              type="number"
              min="1"
              max={totalPages}
              defaultValue={page}
              key={page}
              onChange={() => { }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  const value = e.currentTarget.value;
                  if (value === '') return;
                  const p = parseInt(value);
                  if (!isNaN(p) && p >= 1 && p <= totalPages) {
                    setPage(p);
                  }
                }
              }}
              onBlur={(e) => {
                const value = e.target.value;
                if (value === '' || isNaN(parseInt(value))) {
                  e.target.value = page.toString();
                } else {
                  const p = parseInt(value);
                  if (p >= 1 && p <= totalPages) setPage(p);
                  else e.target.value = page.toString();
                }
              }}
              className="w-16 px-2 py-1 text-sm text-center border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
            <span className="text-sm text-gray-600">页，共 {totalPages} 页</span>
          </div>

          <Button
            variant="outline"
            size="sm"
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
          >
            下一页
          </Button>
        </div>
      )}
    </div>
  );
}
