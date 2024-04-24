<?php

namespace App\Command;

use App\Entity\Queue;
use App\Handler\QueueHandler;
use Gpmd\ShoplineBundle\Dto\Order\Fulfillment;
use Gpmd\ShoplineBundle\Dto\Order\LineItem;
use Gpmd\ShoplineBundle\Dto\Order\Order;
use Gpmd\ShoplineBundle\Dto\Order\PaymentDetail;
use Gpmd\ShoplineBundle\Dto\Order\Return\ReturnF;
use Gpmd\ShoplineBundle\Dto\Order\Return\ReturnLineItem;
use Gpmd\ShoplineBundle\Dto\Order\Returns;
use Gpmd\ShoplineBundle\Interfaces\Handler\OrderApiHandlerInterface;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(
    name: 'process:refund',
    description: 'Process returned items and refund the order items'
)]
class ProcessRefund extends Command
{

    public function __construct(
        private readonly   LoggerInterface              $logger,
        protected readonly ManagerRegistry              $doctrine,
        protected readonly EntityManagerInterface       $entityManager,
        protected readonly OrderApiHandlerInterface     $orderApiHandler,
        protected readonly QueueHandler                 $queueHandler,

    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $repository = $this->doctrine->getRepository(Queue::class);
        $queue = $repository->findUnprocessedReturns();
        if (!$queue) {
            return Command::SUCCESS;
        }
        $queue = $this->mergeQueues($queue);

        /** @var Queue $item */
        foreach ($queue as $salesOrderNumber => $items) {
            try {
                $orderId = $this->queueHandler->getOrderId($salesOrderNumber);
                if (!$orderId) {
                    throw new \Exception(sprintf('Order not found: %s', $salesOrderNumber));
                }
                $order = $this->queueHandler->getOrderDetails($orderId);
                $returns = null;
                try {
                    $returns = $this->orderApiHandler->getReturn($order->getId());
                    $this->handleReturn($order, $items, $returns);
                    $returns = $this->orderApiHandler->getReturn($order->getId());
                }
                catch (\Exception $e) {
                    // no drama
                }
                $this->handleRefund($order, $items, $returns);
                // close return
                if (isset($returns)) {
                    /** @var ReturnF $item */
                    foreach ($returns->getReturns() as $item) {
                        if ($item->getStatus() === 'CLOSED') {
                            continue;
                        }
                        try {
                            $this->orderApiHandler->closeReturn($item->getId());
                        }
                        catch (\Exception $e) {
                            // no drama
                        }
                    }
                }
                foreach ($items['queue_id'] as $queueId) {
                    $item = $this->entityManager->getReference(Queue::class, $queueId);
                    $item->setProcessedAt(new \DateTimeImmutable());
                }
                $this->entityManager->flush();
            } catch (\Exception $e) {
                foreach ($items['queue_id'] as $queueId) {
                    $item = $this->entityManager->getReference(Queue::class, $queueId);
                    $item->setError($e->getMessage());
                }
                $this->entityManager->flush();
                $this->logger->error($e->getMessage());
            }
        }
        return Command::SUCCESS;
    }

    private function handleReturn(Order $order, array $payload, ?Returns $returns): void
    {
        try {
            $used = [];
            if ($returns) {
                /** @var ReturnF $return */
                foreach ($returns->getReturns() as $return) {
                    /** @var ReturnLineItem $returnLineItem */
                    foreach ($return->getReturnLineItems() as $returnLineItem) {
                        $returnedQty = $returnLineItem->getQuantity();
                        $fulfillment = $returnLineItem->getFulfillmentLineItem();
                        $used[$returnLineItem->getFulfillmentId()][$fulfillment->getLineItem()->getSku()] = $returnedQty;
                    }
                }
            }
            $fullfilments = $this->orderApiHandler->getFulfillments($order->getId());
            $returnPayload = [];
            $returnPayload['notify_customer'] = false;
            $returnPayload['order_seq'] = $order->getId();
            $returnPayload['processed_at'] = (new \DateTimeImmutable())->format('Y-m-d\TH:i:sP');
            $returnPayload['return_line_items'] = [];
            foreach ($payload['line_items'] as $sku => $quantities) {
                foreach ($quantities as $qty) {
                    /** @var Fulfillment $fulfillment */
                    foreach ($fullfilments->getFulfillments() as $fulfillment) {
                        if ($qty == 0) {
                            break;
                        }
                        $foundMatch = false;
                        /** @var LineItem $lineItem */
                        foreach ($fulfillment->getLineItems() as $lineItem) {
                            if ($sku != $lineItem->getSku()) {
                                continue;
                            }
                            $usedQty = $used[$fulfillment->getId()][$sku] ?? 0;
                            if ($usedQty > 0) {
                                continue;
                            }
                            if ($lineItem->getFulfillmentQuantity() != $qty) {
                                continue;
                            }
                            $returnPayload['return_line_items'][] = [
                                'fulfillment_id' => $fulfillment->getId(),
                                'line_item_id' => $lineItem->getId(),
                                'quantity' => (int) $qty,
                            ];
                            $used[$fulfillment->getId()][$sku] = $qty;
                            $foundMatch = true;
                            break;
                        }
                        if ($foundMatch) {
                            break;
                        }
                        // fallback
                        foreach ($fulfillment->getLineItems() as $lineItem) {
                            if ($sku != $lineItem->getSku()) {
                                continue;
                            }
                            $usedQty = $used[$fulfillment->getId()][$sku] ?? 0;
                            if ($usedQty >= $lineItem->getFulfillmentQuantity()) {
                                continue;
                            }
                            $free = $lineItem->getFulfillmentQuantity() - $usedQty;
                            $newQty = $qty;
                            if ($qty > $free) {
                                $qty -= $free;
                                $newQty = $free;
                            } else {
                                $qty = 0;
                            }
                            $q = $returnPayload['return_line_items'][$fulfillment->getId()]['quantity'] ?? 0;
                            $returnPayload['return_line_items'][$fulfillment->getId()] = [
                                'fulfillment_id' => $fulfillment->getId(),
                                'line_item_id' => $lineItem->getId(),
                                'quantity' => (int) $newQty + $q,
                            ];
                            $used[$fulfillment->getId()][$sku] = $newQty + $usedQty;
                            if ($qty == 0) {
                                break;
                            }
                        }
                    }
                }
            }
            if (empty($returnPayload['return_line_items'])) {
                return;
            }
            $returnPayload['return_line_items'] = array_values($returnPayload['return_line_items']);
            $this->orderApiHandler->returnOrder($returnPayload);
        }
        catch (\Exception $e) {
            throw new \Exception($e->getMessage());
        }
    }

    private function handleRefund(Order $order, array $payload, ?Returns $returns): void
    {
        if ($order->getFinancialStatus() !== 'paid' && $order->getFinancialStatus() !== 'partially_paid' && $order->getFinancialStatus() !== 'partially_refunded') {
            return; // nothing to refund
        }

        /** @var LineItems $lineItem */
        $refundLineItems = [];
        $amount = 0;
        foreach ($order->getLineItems() as $lineItem) {
            $itemCode = $lineItem->getSku();
            if (!isset($payload['line_items'][$itemCode])) {
                continue;
            }
            $refundLineItems[] = [
                'line_item_id' => $lineItem->getId(),
                'location_id' => $lineItem->getLocationId(),
                'quantity' => array_sum($payload['line_items'][$itemCode]),
                'restock_type' => 'no-restock',
                'type' => 'Returned',
            ];
            $discount = 0;
            if ($lineItem->getDiscountAllocations() !== null) {
                foreach ($lineItem->getDiscountAllocations() as $discountAllocation) {
                    $discount += $discountAllocation->getAmount();
                }
            }
            $amount += ($lineItem->getPrice() - $discount) * array_sum($payload['line_items'][$itemCode]);
        }

        if ($order->getTotalShippingPriceSet()->getShopMoney()->getAmount() > 0 && $this->getShippiedQty($order) === $this->getReturnedQty($returns)) {
            $amount += $order->getTotalShippingPriceSet()->getShopMoney()->getAmount();
        }

        $paymentDetails = [];
        /** @var PaymentDetail $paymentDetail */
        foreach ($order->getPaymentDetails() as $paymentDetail) {
            if ($paymentDetail->getPayStatus() !== 'paid') {
                continue;
            }
            $paymentDetails[] = [
                'amount' => (string) $amount,
                'currency' => '',
                'gateway' => $paymentDetail->getPayChannel(),
                'id' => $paymentDetail->getPaySeq(),
                'kind' => 'refund',
                'type' => true
            ];
        }
        $payload = [
            'order_id' => $order->getId(),
            'currency' => '',
            'note' => 'Refund',
            'notify' => true,
            'refund_line_items' => $refundLineItems,
            'transactions' => $paymentDetails
        ];
        if ($order->getTotalShippingPriceSet()->getShopMoney()->getAmount() > 0 && $this->getShippiedQty($order) === $this->getReturnedQty($returns)){
            $payload['shipping_refund'] = [
                'full_refund' => true,
                $order->getTotalShippingPriceSet()->getShopMoney()->getAmount()
            ];
        }

        $this->orderApiHandler->refundOrder($payload);
    }

    public function mergeQueues(array $queue): array
    {
        $merged = [];
        foreach ($queue as $item) {
            $payload = json_decode($item->getPayload(), true);
            $items = explode(',', $payload['items']);
            $qty = explode(',', $payload['qty']);
            foreach ($items as $key => $sku) {
                if (!isset($merged[$item->getSalesOrderNumber()]['line_items'][$sku])) {
                    $merged[$item->getSalesOrderNumber()]['line_items'][$sku] = [];
                }
                $merged[$item->getSalesOrderNumber()]['line_items'][$sku][] = (int) $qty[$key];
                $merged[$item->getSalesOrderNumber()]['queue_id'][] = $item->getId();
            }
        }
        return $merged;
    }

    private function getShippiedQty(Order $order): int
    {
        $orderedQty = 0;
        /** @var Fulfillment $fulfillment */
        foreach ($order->getFulfillments() as $fulfillment) {
            foreach ($fulfillment->getLineItems() as $lineItem) {
                $orderedQty += $lineItem->getFulfillmentQuantity();
            }
        }
        return $orderedQty;
    }

    private function getReturnedQty(?Returns $returns): ?int
    {
        if (!$returns) {
            return 0;
        }
        $returnedQty = 0;
        foreach ($returns->getReturns() as $return) {
            foreach ($return->getReturnLineItems() as $returnLineItem) {
                $returnedQty += $returnLineItem->getQuantity();
            }
        }
        return $returnedQty;
    }

}
