import { test, expect } from '@playwright/test'

test('tools page renders key sections', async ({ page }) => {
  await page.route('**/pbi/instances', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ instances: [], available: true }),
    })
  })

  await page.goto('/tools')

  await expect(page.getByRole('heading', { name: 'DAX Tools' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Optimize DAX (LLM)' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Power BI Desktop' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Validate DAX (PBI Desktop)' })).toBeVisible()

  await expect(page.getByRole('button', { name: 'Start Optimization' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Refresh Instances' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Validate' })).toBeVisible()
})
