export type ReviewCard = {
  title: string;
  status: "open" | "closed";
};

export function renderReviewCard(card: ReviewCard): string {
  return `${card.status.toUpperCase()}: ${card.title}`;
}
