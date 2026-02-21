'use client';

import type { ReactNode } from 'react';
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';

export const middlePanelTokens = {
  panelCard: 'border border-gray-200 shadow-none overflow-hidden h-full flex flex-col',
  tabHeader: 'pb-0 pt-3 px-4 border-b border-gray-100 bg-gray-50/50',
  tabsList: 'grid w-full bg-transparent p-0 h-auto gap-2 space-x-0',
  tabTrigger:
    'flex items-center justify-center gap-2 focus:outline-none focus-visible:outline-none focus-visible:ring-0',
  content: 'px-4 pt-4 pb-2 flex-1 min-h-0 flex flex-col',
  logSurface: 'h-full min-h-[70vh] overflow-hidden rounded-lg border border-gray-200 bg-gradient-to-br from-slate-50 to-gray-100',
};

export interface MiddlePanelTabItem {
  value: string;
  label: ReactNode;
  content: ReactNode;
  contentClassName?: string;
}

interface MiddlePanelShellProps {
  value: string;
  onValueChange: (value: string) => void;
  tabs: MiddlePanelTabItem[];
  className?: string;
}

export default function MiddlePanelShell({
  value,
  onValueChange,
  tabs,
  className,
}: MiddlePanelShellProps) {
  return (
    <Card className={cn(middlePanelTokens.panelCard, className)}>
      <Tabs value={value} onValueChange={onValueChange} className="flex-1 flex flex-col min-h-0">
        <CardHeader className={middlePanelTokens.tabHeader}>
          <TabsList
            className={middlePanelTokens.tabsList}
            style={{ gridTemplateColumns: `repeat(${Math.max(1, tabs.length)}, minmax(0, 1fr))` }}
          >
            {tabs.map((tab) => (
              <TabsTrigger
                key={tab.value}
                value={tab.value}
                className={middlePanelTokens.tabTrigger}
              >
                {tab.label}
              </TabsTrigger>
            ))}
          </TabsList>
        </CardHeader>

        <CardContent className={middlePanelTokens.content}>
          {tabs.map((tab) => (
            <TabsContent
              key={tab.value}
              value={tab.value}
              className={cn('mt-0 h-full flex flex-col min-h-0', tab.contentClassName)}
            >
              {tab.content}
            </TabsContent>
          ))}
        </CardContent>
      </Tabs>
    </Card>
  );
}
